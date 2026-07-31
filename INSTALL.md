# Установка Cyrius Stream Control

## Новый сервер

```bash
apt-get update
apt-get install -y git docker.io curl
systemctl enable --now docker

git clone git@github-flussonic:soc4upl/flussonic-panel.git /root/flussonic-panel
cd /root/flussonic-panel
cp .env.example .env
nano .env
chmod 600 .env
chmod +x run-docker.sh
./run-docker.sh
```

Проверка:

```bash
curl -s http://127.0.0.1:8088/api/health
docker logs --tail=100 flussonic-panel
```

Панель: `http://IP_СЕРВЕРА:8088/?v=58`.

## Обновление из GitHub

```bash
cd /root/flussonic-panel
git pull --ff-only
./run-docker.sh
```

## Полный перенос данных

На старом сервере:

```bash
cp /root/flussonic-panel/.env /root/flussonic-panel.env.backup
docker run --rm -v flussonic-panel-data:/data:ro -v /root:/backup alpine \
  tar czf /backup/flussonic-panel-data.tar.gz -C /data .
```

На новом сервере после `git clone`:

```bash
cp /root/flussonic-panel.env.backup /root/flussonic-panel/.env
docker volume create flussonic-panel-data
docker run --rm -v flussonic-panel-data:/data -v /root:/backup alpine \
  tar xzf /backup/flussonic-panel-data.tar.gz -C /data
cd /root/flussonic-panel
./run-docker.sh
```

Не меняйте `PANEL_SECRET_KEY`, иначе сохранённые пароли и `cluster_key` не расшифруются.
