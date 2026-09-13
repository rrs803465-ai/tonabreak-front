#!/usr/bin/env bash
# ==============================================================================
# DAYBREAK - Client Environment Setup & Background Service Launcher
# ==============================================================================

set -euo pipefail

# Visual styling
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. System Package Installation
log_info "Installing system dependencies..."

if command -v apt-get &>/dev/null; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3 python3-pip python3-venv proxychains4 curl procps net-tools
elif command -v apk &>/dev/null; then
    sudo apk update && sudo apk add python3 py3-pip proxychains-ng curl procps net-tools
elif command -v dnf &>/dev/null || command -v yum &>/dev/null; then
    PKG_MGR=$(command -v dnf || command -v yum)
    sudo $PKG_MGR install -y python3 python3-pip proxychains-ng curl procps net-tools
fi

# 2. Kernel Network Socket Optimization
log_info "Applying Linux kernel network stack optimizations..."
TUNING_CONF="/etc/sysctl.d/99-daybreak-network.conf"
cat << 'EOF' | sudo tee $TUNING_CONF > /dev/null
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65536
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_keepalive_time = 30
net.ipv4.tcp_keepalive_intvl = 5
net.ipv4.tcp_keepalive_probes = 5
net.ipv4.ip_local_port_range = 1024 65535
EOF

if command -v sysctl &>/dev/null; then
    sudo sysctl -p $TUNING_CONF &>/dev/null || log_warn "Container environment detected; skipping kernel tuning."
fi

# 3. Virtual Environment Setup
VENV_DIR="$SCRIPT_DIR/venv"
log_info "Creating Python virtual environment in $VENV_DIR..."
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

log_info "Installing Python dependencies (websockets, uvloop, psutil)..."
pip install --upgrade pip setuptools wheel --quiet
pip install websockets uvloop psutil --quiet

# 4. Proxychains Configuration
PROXYCHAINS_CONF="/etc/proxychains4.conf"
if [ ! -w "$PROXYCHAINS_CONF" ] && [ ! -f "$PROXYCHAINS_CONF" ]; then
    PROXYCHAINS_CONF="$HOME/.proxychains/proxychains.conf"
    mkdir -p "$(dirname "$PROXYCHAINS_CONF")"
fi

log_info "Configuring Proxychains at $PROXYCHAINS_CONF..."
cat << 'EOF' > "$PROXYCHAINS_CONF"
strict_chain
proxy_dns
remote_dns_subnet 224
tcp_read_time_out 15000
tcp_connect_time_out 8000

[ProxyList]
socks5 127.0.0.1 1080
EOF

# 5. Background Process Launch
CLIENT_FILE="$SCRIPT_DIR/client.py"
LOG_FILE="$SCRIPT_DIR/client_out.log"
PID_FILE="$SCRIPT_DIR/client.pid"

if [ ! -f "$CLIENT_FILE" ]; then
    log_error "client.py not found in $SCRIPT_DIR."
    exit 1
fi

log_info "Executing client.py in background using nohup..."
nohup "$VENV_DIR/bin/python3" "$CLIENT_FILE" > "$LOG_FILE" 2>&1 &
CLIENT_PID=$!
echo "$CLIENT_PID" > "$PID_FILE"

# Verification
sleep 2
if ps -p "$CLIENT_PID" > /dev/null 2>&1; then
    log_success "client.py is successfully running in background (PID: $CLIENT_PID)."
    log_info "Log output directed to: $LOG_FILE"
    log_info "To follow live output run: tail -f $LOG_FILE"
else
    log_error "client.py failed to stay active. Check logs in $LOG_FILE for errors."
    exit 1
fi
