import asyncio
import logging
import socket
import struct
import sys
import time
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

RAILWAY_WSS_URL = sys.argv[1] if len(sys.argv) > 1 else "wss://daybreak-production-393b.up.railway.app/" # Change the url to yur railway url, else use mine. access the repo of backend at https://github.com/rrs803465-ai/daybreak, deploy at railway
BIND_HOST = "0.0.0.0"
BIND_PORT = 1080

# Protocol Constants (Matches server.py)
CMD_CONNECT   = 0x01
CMD_DATA      = 0x02
CMD_CLOSE     = 0x03
CMD_CONNECTED = 0x04
CMD_ERROR     = 0x05
CMD_PING      = 0x06
CMD_PONG      = 0x07

HEADER_FORMAT = ">IBI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class ResilientClientEngine:
    def __init__(self, wss_url):
        self.wss_url = wss_url
        self.ws = None
        self.stream_counter = 0
        self.streams = {}           # stream_id -> (reader, writer)
        self.connect_events = {}    # stream_id -> asyncio.Event()
        self.connect_status = {}    # stream_id -> bool
        self.is_connected = False
        self.lock = asyncio.Lock()

    async def run_wss_loop(self):
        """Maintains an unbroken WSS tunnel using exponential backoff."""
        backoff = 1
        while True:
            try:
                logging.info(f"Establishing WSS control tunnel to {self.wss_url}...")
                async with websockets.connect(
                    self.wss_url,
                    ping_interval=12,
                    ping_timeout=30,
                    max_size=None,
                    write_limit=2097152
                ) as ws:
                    self.ws = ws
                    self.is_connected = True
                    backoff = 1
                    logging.info("WSS Control Tunnel ACTIVE and connected.")

                    receiver_task = asyncio.create_task(self._process_wss_frames())
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop())

                    await asyncio.gather(receiver_task, heartbeat_task)

            except (websockets.exceptions.ConnectionClosed, OSError, Exception) as e:
                self.is_connected = False
                self.ws = None
                logging.warning(f"WSS Tunnel dropped ({e}). Reconnecting in {backoff}s...")
                await self._cleanup_all_streams()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 15)

    async def _heartbeat_loop(self):
        try:
            while self.is_connected and self.ws:
                await asyncio.sleep(8)
                ping_frame = struct.pack(HEADER_FORMAT, 0, CMD_PING, 0)
                await self.ws.send(ping_frame)
        except Exception:
            pass

    async def _process_wss_frames(self):
        try:
            async for message in self.ws:
                if not isinstance(message, bytes) or len(message) < HEADER_SIZE:
                    continue

                stream_id, cmd, payload_len = struct.unpack(
                    HEADER_FORMAT, message[:HEADER_SIZE]
                )
                payload = message[HEADER_SIZE : HEADER_SIZE + payload_len]

                if cmd == CMD_CONNECTED:
                    if stream_id in self.connect_events:
                        self.connect_status[stream_id] = True
                        self.connect_events[stream_id].set()

                elif cmd == CMD_ERROR:
                    if stream_id in self.connect_events:
                        self.connect_status[stream_id] = False
                        self.connect_events[stream_id].set()
                    await self._close_stream(stream_id)

                elif cmd == CMD_DATA:
                    async with self.lock:
                        stream = self.streams.get(stream_id)
                    if stream:
                        _, writer = stream
                        try:
                            writer.write(payload)
                            await writer.drain()
                        except Exception:
                            await self._close_stream(stream_id)

                elif cmd == CMD_CLOSE:
                    if stream_id in self.connect_events:
                        self.connect_events[stream_id].set()
                    await self._close_stream(stream_id)

        except Exception as e:
            logging.error(f"Error processing WSS frames: {e}")

    async def handle_socks_client(self, reader, writer):
        """Processes local inbound SOCKS5 proxy connections."""
        sock = writer.get_extra_info("socket")
        if sock:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass

        try:
            # 1. SOCKS5 Auth Handshake
            header = await reader.readexactly(2)
            ver, nmethods = header[0], header[1]
            await reader.readexactly(nmethods)
            writer.write(b"\x05\x00")  # No Auth
            await writer.drain()

            # 2. Parse Connection Request
            req = await reader.readexactly(4)
            cmd, atyp = req[1], req[3]

            if cmd != 1:  # Only CONNECT supported
                writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                writer.close()
                return

            if atyp == 1:    # IPv4
                addr = await reader.readexactly(4)
                host = ".".join(map(str, addr))
            elif atyp == 3:  # Domain (socks5h)
                length = (await reader.readexactly(1))[0]
                host = (await reader.readexactly(length)).decode("utf-8", errors="ignore")
            elif atyp == 4:  # IPv6
                addr = await reader.readexactly(16)
                host = ":".join(f"{addr[i]<<8 | addr[i+1]:x}" for i in range(0, 16, 2))
            else:
                writer.close()
                return

            port = struct.unpack(">H", await reader.readexactly(2))[0]

            if not self.is_connected or not self.ws:
                writer.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00")  # General failure
                await writer.drain()
                writer.close()
                return

            # Allocate new unique Stream ID
            async with self.lock:
                self.stream_counter = (self.stream_counter + 1) % 4294967295
                stream_id = self.stream_counter
                self.streams[stream_id] = (reader, writer)

            self.connect_events[stream_id] = asyncio.Event()
            self.connect_status[stream_id] = False

            # Send CMD_CONNECT over WSS
            host_bytes = host.encode("utf-8")
            payload = struct.pack(">HB", port, len(host_bytes)) + host_bytes
            frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CONNECT, len(payload)) + payload

            await self.ws.send(frame)

            # Wait for Railway confirmation frame
            try:
                await asyncio.wait_for(self.connect_events[stream_id].wait(), timeout=12.0)
            except asyncio.TimeoutError:
                writer.write(b"\x05\x04\x00\x01\x00\x00\x00\x00\x00\x00")  # Host unreachable
                await writer.drain()
                await self._close_stream(stream_id)
                return

            if not self.connect_status.get(stream_id, False):
                writer.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")  # Connection refused
                await writer.drain()
                await self._close_stream(stream_id)
                return

            # Send SOCKS5 Success Response
            writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()

            # Pump TCP Client Bytes -> WSS Tunnel
            try:
                while True:
                    data = await reader.read(65536)
                    if not data:
                        break
                    if self.is_connected and self.ws:
                        header = struct.pack(HEADER_FORMAT, stream_id, CMD_DATA, len(data))
                        await self.ws.send(header + data)
            except Exception:
                pass
            finally:
                if self.is_connected and self.ws:
                    close_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CLOSE, 0)
                    try:
                        await self.ws.send(close_frame)
                    except Exception:
                        pass
                await self._close_stream(stream_id)

        except Exception:
            try:
                writer.close()
            except Exception:
                pass

    async def _close_stream(self, stream_id):
        self.connect_events.pop(stream_id, None)
        self.connect_status.pop(stream_id, None)
        async with self.lock:
            stream = self.streams.pop(stream_id, None)

        if stream:
            _, writer = stream
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _cleanup_all_streams(self):
        async with self.lock:
            sids = list(self.streams.keys())
        for sid in sids:
            await self._close_stream(sid)


async def main():
    engine = ResilientClientEngine(RAILWAY_WSS_URL)
    asyncio.create_task(engine.run_wss_loop())

    server = await asyncio.start_server(engine.handle_socks_client, BIND_HOST, BIND_PORT)
    logging.info(f"Resilient SOCKS5 Listener running on {BIND_HOST}:{BIND_PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Client daemon stopped.")
