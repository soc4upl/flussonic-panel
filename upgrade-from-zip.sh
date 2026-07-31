#!/usr/bin/env bash
set -Eeuo pipefail

ARCHIVE="${1:-/root/flussonic-panel-v5.9.1.zip}"
TARGET="${2:-/root/flussonic-panel}"
TMP="$(mktemp -d /tmp/flussonic-panel-v591.XXXXXX)"
ENV_BACKUP="/root/flussonic-panel.env.v591.backup"
trap 'rm -rf "$TMP"' EXIT

[[ -f "$ARCHIVE" ]] || { echo "Архив не найден: $ARCHIVE" >&2; exit 1; }
[[ -d "$TARGET" ]] || { echo "Папка панели не найдена: $TARGET" >&2; exit 1; }
[[ -f "$TARGET/.env" ]] || { echo "Файл .env не найден: $TARGET/.env" >&2; exit 1; }

unzip -q -o "$ARCHIVE" -d "$TMP"
SOURCE="$TMP/flussonic-panel"
[[ -f "$SOURCE/app/main.py" ]] || { echo "В архиве нет app/main.py" >&2; exit 1; }
grep -q '5.9.1' "$SOURCE/app/main.py" || { echo "Архив не содержит v5.9.1" >&2; exit 1; }

cp "$TARGET/.env" "$ENV_BACKUP"
docker rm -f flussonic-panel >/dev/null 2>&1 || true

cd "$TARGET"
find . -mindepth 1 -maxdepth 1 ! -name '.git' ! -name '.env' -exec rm -rf -- {} +
cp -a "$SOURCE/." "$TARGET/"
cp "$ENV_BACKUP" "$TARGET/.env"
chmod 600 "$TARGET/.env"
chmod +x "$TARGET/run-docker.sh" "$TARGET/upgrade-from-zip.sh"

cd "$TARGET"
./run-docker.sh
