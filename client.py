# ==============================================================================
# client.py - High-Resilience SOCKS5 Local Client & Tunnel Agent
# Creates local SOCKS5 proxy on 127.0.0.1:1080 and multiplexes all streams over WSS
# ==============================================================================

import asyncio
import logging
import os
import socket
import struct
import sys
import time
from typing import Dict, Tuple, Optional

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] [CLIENT] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Environment variables & runtime parameters
WSS_SERVER_URL = os.environ.get("WSS_SERVER_URL", "wss://your-app.up.railway.app")
SOCKS_BIND_HOST = os.environ.get("SOCKS_HOST", "127.0.0.1")
SOCKS_BIND_PORT = int(os.environ.get("SOCKS_PORT", 1080))

READ_CHUNK_SIZE = 64 * 1024
HEADER_FORMAT = ">IBI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Command Standard Identifiers
CMD_CONNECT   = 0x01
CMD_DATA      = 0x02
CMD_CLOSE     = 0x03
CMD_CONNECTED = 0x04
CMD_ERROR     = 0x05
CMD_PING      = 0x06
CMD_PONG      = 0x07


class ClientPerformanceStats:
    def __init__(self):
        self.bytes_sent = 0
        self.bytes_received = 0
        self.active_local_connections = 0

    def stats_summary(self) -> str:
        return (
            f"Active SOCKS Connections: {self.active_local_connections} | "
            f"TX: {self.bytes_sent / (1024 * 1024):.2f} MB | "
            f"RX: {self.bytes_received / (1024 * 1024):.2f} MB"
        )


client_stats = ClientPerformanceStats()


class DaybreakClientEngine:
    def __init__(self, target_url: str):
        self.target_url = target_url
        self.websocket = None
        self.send_lock = asyncio.Lock()
        
        # State registries
        self.local_streams: Dict[int, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self.pending_connect_events: Dict[int, asyncio.Event] = {}
        self.connect_outcomes: Dict[int, bool] = {}
        
        self.next_stream_id = 1
        self.id_lock = asyncio.Lock()
        self.global_registry_lock = asyncio.Lock()

    async def generate_stream_id(self) -> int:
        async with self.id_lock:
            sid = self.next_stream_id
            self.next_stream_id = (self.next_stream_id + 1) & 0xFFFFFFFF
            if self.next_stream_id == 0:
                self.next_stream_id = 1
            return sid

    async def transmit_frame(self, data: bytes):
        """Thread-safe output frame dispatch to remote WSS server."""
        async with self.send_lock:
            if self.websocket and self.websocket.open:
                try:
                    await self.websocket.send(data)
                    client_stats.bytes_sent += len(data)
                except Exception as ex:
                    logging.error(f"WSS send frame failure: {ex}")

    async def main_reconnect_loop(self):
        """Persistent connection manager ensuring automatic reconnects."""
        retry_delay = 1.0
        while True:
            try:
                logging.info(f"Establishing persistent WSS session -> {self.target_url}")
                async with websockets.connect(
                    self.target_url,
                    ping_interval=10,
                    ping_timeout=25,
                    max_size=None,
                    write_limit=8192192
                ) as ws:
                    self.websocket = ws
                    retry_delay = 1.0  # Reset retry delay on successful connection
                    logging.info("Connected to WSS Server endpoint successfully.")
                    
                    # Spawn active ping monitor
                    ping_task = asyncio.create_task(self._send_ping_loop())
                    try:
                        await self._wss_receive_loop()
                    finally:
                        ping_task.cancel()

            except Exception as error:
                logging.warning(f"Connection lost ({error}). Reconnecting in {retry_delay:.1f}s...")
                await self._purge_all_local_streams()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 15.0)

    async def _send_ping_loop(self):
        """Periodic keepalive ping to prevent aggressive firewall drop."""
        while True:
            await asyncio.sleep(8)
            ping_frame = struct.pack(HEADER_FORMAT, 0, CMD_PING, 0)
            await self.transmit_frame(ping_frame)

    async def _wss_receive_loop(self):
        """Decodes raw WebSocket stream packets and dispatches to local target routines."""
        try:
            async for frame in self.websocket:
                if not isinstance(frame, bytes) or len(frame) < HEADER_SIZE:
                    continue

                client_stats.bytes_received += len(frame)
                stream_id, cmd, payload_len = struct.unpack(HEADER_FORMAT, frame[:HEADER_SIZE])
                payload = frame[HEADER_SIZE : HEADER_SIZE + payload_len]

                if cmd == CMD_CONNECTED:
                    if stream_id in self.pending_connect_events:
                        self.connect_outcomes[stream_id] = True
                        self.pending_connect_events[stream_id].set()

                elif cmd == CMD_ERROR:
                    logging.error(f"[Stream {stream_id}] Remote server rejected connection.")
                    if stream_id in self.pending_connect_events:
                        self.connect_outcomes[stream_id] = False
                        self.pending_connect_events[stream_id].set()

                elif cmd == CMD_DATA:
                    asyncio.create_task(self._write_to_local_socket(stream_id, payload))

                elif cmd == CMD_CLOSE:
                    asyncio.create_task(self._close_local_socket(stream_id))

        except websockets.exceptions.ConnectionClosed:
            logging.warning("WSS payload receive loop closed.")

    async def handle_socks_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Complete RFC 1928 SOCKS5 Protocol Engine."""
        stream_id = await self.generate_stream_id()

        try:
            # 1. SOCKS5 Greeting Protocol
            version_byte = await reader.readexactly(1)
            if version_byte != b"\x05":
                writer.close()
                return

            nmethods_byte = await reader.readexactly(1)
            nmethods = nmethods_byte[0]
            _ = await reader.readexactly(nmethods)

            # Response: NO AUTH REQUIRED
            writer.write(b"\x05\x00")
            await writer.drain()

            # 2. SOCKS5 Command Request
            req_header = await reader.readexactly(4)
            ver, cmd, rsv, atyp = req_header[0], req_header[1], req_header[2], req_header[3]

            if cmd != 1:  # Command 1 = TCP CONNECT
                writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                writer.close()
                return

            # Address parsing
            if atyp == 1:    # IPv4
                addr_bytes = await reader.readexactly(4)
                dest_host = socket.inet_ntoa(addr_bytes)
            elif atyp == 3:  # Domain Name
                domain_len = (await reader.readexactly(1))[0]
                domain_bytes = await reader.readexactly(domain_len)
                dest_host = domain_bytes.decode("utf-8")
            elif atyp == 4:  # IPv6
                addr_bytes = await reader.readexactly(16)
                dest_host = socket.inet_ntop(socket.AF_INET6, addr_bytes)
            else:
                writer.close()
                return

            port_bytes = await reader.readexactly(2)
            dest_port = struct.unpack(">H", port_bytes)[0]

            # Build CMD_CONNECT payload
            host_encoded = dest_host.encode("utf-8")
            connect_payload = struct.pack(">HB", dest_port, len(host_encoded)) + host_encoded
            connect_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CONNECT, len(connect_payload)) + connect_payload

            event = asyncio.Event()
            self.pending_connect_events[stream_id] = event
            self.connect_outcomes[stream_id] = False

            await self.transmit_frame(connect_frame)

            # Wait for remote ack
            try:
                await asyncio.wait_for(event.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                logging.error(f"[Stream {stream_id}] Target connection timeout to {dest_host}:{dest_port}")
                writer.write(b"\x05\x04\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                writer.close()
                return
            finally:
                self.pending_connect_events.pop(stream_id, None)

            if not self.connect_outcomes.get(stream_id, False):
                writer.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                writer.close()
                return

            # Reply Success
            writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()

            async with self.global_registry_lock:
                self.local_streams[stream_id] = (reader, writer)
                client_stats.active_local_connections += 1

            # Begin Local -> WSS payload forwarding
            await self._pump_local_to_wss(stream_id, reader)

        except Exception as err:
            logging.debug(f"[Stream {stream_id}] Local SOCKS session error: {err}")
        finally:
            await self._close_local_socket(stream_id)

    async def _pump_local_to_wss(self, stream_id: int, reader: asyncio.StreamReader):
        try:
            while True:
                data = await reader.read(READ_CHUNK_SIZE)
                if not data:
                    break

                header = struct.pack(HEADER_FORMAT, stream_id, CMD_DATA, len(data))
                await self.transmit_frame(header + data)

        except Exception:
            pass
        finally:
            close_frame = struct.pack(HEADER_FORMAT, stream_id, CMD_CLOSE, 0)
            await self.transmit_frame(close_frame)

    async def _write_to_local_socket(self, stream_id: int, payload: bytes):
        async with self.global_registry_lock:
            stream = self.local_streams.get(stream_id)

        if stream:
            _, writer = stream
            try:
                writer.write(payload)
                await writer.drain()
            except Exception:
                await self._close_local_socket(stream_id)

    async def _close_local_socket(self, stream_id: int):
        async with self.global_registry_lock:
            stream = self.local_streams.pop(stream_id, None)
            if stream:
                client_stats.active_local_connections -= 1

        if stream:
            _, writer = stream
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _purge_all_local_streams(self):
        async with self.global_registry_lock:
            sids = list(self.local_streams.keys())
        for sid in sids:
            await self._close_local_socket(sid)

    async def periodic_stats_logger(self):
        while True:
            await asyncio.sleep(20)
            logging.info(f"CLIENT STATS -> {client_stats.stats_summary()}")


async def main():
    if "your-app" in WSS_SERVER_URL:
        logging.warning("Please specify WSS_SERVER_URL env var before executing.")

    engine = DaybreakClientEngine(WSS_SERVER_URL)

    # Spawn background tunnel & logger tasks
    asyncio.create_task(engine.main_reconnect_loop())
    asyncio.create_task(engine.periodic_stats_logger())

    socks_server = await asyncio.start_server(
        engine.handle_socks_connection,
        SOCKS_BIND_HOST,
        SOCKS_BIND_PORT
    )

    logging.info(f"SOCKS5 local listener started on {SOCKS_BIND_HOST}:{SOCKS_BIND_PORT}")
    async with socks_server:
        await socks_server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Client shutdown sequence complete.")
