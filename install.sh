#!/usr/bin/env bash
# ProxyHub - instalare Ubuntu/Debian (systemd)
# curl -fsSL https://raw.githubusercontent.com/maxvideostar/ProxyHub/main/install.sh | sudo bash
set -e
[ "$(id -u)" = 0 ] || { echo "Ruleaza cu sudo"; exit 1; }
DIR=/opt/proxyhub
REPO=https://raw.githubusercontent.com/maxvideostar/ProxyHub/main/bin
case "$(uname -m)" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "Arhitectura nesuportata: $(uname -m)"; exit 1 ;;
esac
mkdir -p "$DIR"
if [ -f "$(dirname "$0")/bin/proxyhub-linux-$ARCH" ]; then
  cp "$(dirname "$0")/bin/proxyhub-linux-$ARCH" "$DIR/proxyhub.new"
else
  curl -fsSL "$REPO/proxyhub-linux-$ARCH" -o "$DIR/proxyhub.new"
fi
chmod +x "$DIR/proxyhub.new"
if systemctl list-unit-files 2>/dev/null | grep -q '^ProxyHub.service'; then
  systemctl stop ProxyHub || true
  mv -f "$DIR/proxyhub.new" "$DIR/proxyhub"
  systemctl start ProxyHub
  echo "ProxyHub actualizat."
else
  mv -f "$DIR/proxyhub.new" "$DIR/proxyhub"
  "$DIR/proxyhub" -c "$DIR/proxyhub.json" install
  "$DIR/proxyhub" -c "$DIR/proxyhub.json" start
fi
PORT=$(grep -o '"listen": *"[^"]*"' "$DIR/proxyhub.json" 2>/dev/null | grep -o '[0-9]*"$' | tr -d '"'); PORT=${PORT:-8090}
if command -v ufw >/dev/null && ufw status | grep -q active; then ufw allow "$PORT"/tcp >/dev/null && echo "ufw: portul $PORT deschis"; fi
IP=$(hostname -I 2>/dev/null | awk '{print $1}')
echo
echo "==============================================="
echo " ProxyHub ruleaza:  http://${IP:-localhost}:$PORT/panel"
echo " Login implicit:    admin / admin  (schimba-l!)"
echo " Config:            $DIR/proxyhub.json"
echo " Log:               $DIR/proxyhub.log  |  journalctl -u ProxyHub -f"
echo "==============================================="
