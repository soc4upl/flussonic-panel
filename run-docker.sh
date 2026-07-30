#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  echo "Файл .env не найден. Скопируйте .env.example в .env и настройте панель." >&2
  exit 1
fi

IMAGE="flussonic-panel:v5"
CONTAINER="flussonic-panel"
VOLUME="flussonic-panel-data"

echo "Сборка Cyrius Stream Control v5 без Docker-кэша..."
docker build --no-cache -t "$IMAGE" .
docker volume create "$VOLUME" >/dev/null
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true

docker run -d \
  --name "$CONTAINER" \
  --restart unless-stopped \
  --env-file "$(pwd)/.env" \
  -v "$VOLUME:/app/data" \
  -p 8088:8088 \
  "$IMAGE"

echo
echo "Контейнер запущен. Проверка версии:"
for _ in $(seq 1 20); do
  if docker exec "$CONTAINER" python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8088/api/health', timeout=2).read().decode())" 2>/dev/null; then
    break
  fi
  sleep 1
done

echo "Логи: docker logs -f $CONTAINER"
echo "Адрес: http://$(hostname -I 2>/dev/null | awk '{print $1}'):8088"
