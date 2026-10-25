#!/usr/bin/env bash
# Enables files up to 2 GB by running Telegram's own Bot API server on this VPS.
# Run AFTER setup.sh:   bash bigfiles.sh
set -e
cd "$(dirname "$0")"
DIR="$(pwd)"
SERVICE="zipbot-$(basename "$DIR" | tr -c 'A-Za-z0-9\n' '-')"

[ -f .env ] || { echo ".env not found. Run setup.sh first."; exit 1; }
BOT_TOKEN="$(grep '^BOT_TOKEN=' .env | cut -d= -f2-)"

echo "Get api_id and api_hash at https://my.telegram.org -> API development tools"
read -rp "api_id: " API_ID
read -rp "api_hash: " API_HASH

if ! command -v docker >/dev/null 2>&1; then
  echo "== Installing Docker =="
  curl -fsSL https://get.docker.com | sudo sh
fi

echo "== Logging the bot out of Telegram's public server =="
curl -s "https://api.telegram.org/bot${BOT_TOKEN}/logOut" || true
echo

echo "== Starting local Bot API server =="
sudo mkdir -p /var/lib/telegram-bot-api
sudo docker rm -f telegram-bot-api >/dev/null 2>&1 || true
sudo docker run -d --name telegram-bot-api --restart always \
  -p 127.0.0.1:8081:8081 \
  -e TELEGRAM_API_ID="$API_ID" -e TELEGRAM_API_HASH="$API_HASH" -e TELEGRAM_LOCAL=1 \
  -v /var/lib/telegram-bot-api:/var/lib/telegram-bot-api \
  aiogram/telegram-bot-api:latest

grep -q '^LOCAL_API_URL=' .env || echo "LOCAL_API_URL=http://localhost:8081" >> .env

echo "== Letting the bot read the server's files =="
# The Bot API server stores downloads as a private user, so the bot service runs as root.
sudo sed -i 's/^User=.*/User=root/' /etc/systemd/system/$SERVICE.service
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE"
sleep 3
sudo systemctl --no-pager status "$SERVICE" | head -n 5
echo
echo "Done. Files up to 2 GB now work. Send a large file to your bot to test."
