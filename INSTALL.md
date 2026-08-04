# Установка Cyrius Stream Control v5.9.4

## Новый сервер

```bash
apt-get update
apt-get install -y git docker.io curl unzip
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

Ожидаемая версия: `5.9.4`. Панель: `http://IP_СЕРВЕРА:8088/?v=594`.

## Обновление из GitHub

```bash
cd /root/flussonic-panel
git pull --ff-only
./run-docker.sh
```

## Обновление из ZIP без rsync

Архив должен находиться в `/root/flussonic-panel-v5.9.4.zip`.

```bash
rm -rf /tmp/flussonic-panel-v594
mkdir -p /tmp/flussonic-panel-v594
unzip -q -o /root/flussonic-panel-v5.9.4.zip -d /tmp/flussonic-panel-v594

cp /root/flussonic-panel/.env /root/flussonic-panel.env.backup
docker rm -f flussonic-panel 2>/dev/null || true

cd /root/flussonic-panel
find . -mindepth 1 -maxdepth 1 ! -name '.git' ! -name '.env' -exec rm -rf -- {} +
cp -a /tmp/flussonic-panel-v594/flussonic-panel/. /root/flussonic-panel/
cp /root/flussonic-panel.env.backup /root/flussonic-panel/.env
chmod 600 /root/flussonic-panel/.env
chmod +x /root/flussonic-panel/run-docker.sh

cd /root/flussonic-panel
./run-docker.sh
```

Проверка:

```bash
docker inspect flussonic-panel --format '{{.Config.Image}}'
curl -s http://127.0.0.1:8088/api/health
echo
```

Образ должен быть `flussonic-panel:v594`.

## Первое включение распределения

1. Откройте **Размещение**.
2. Включите галочку **Гибридное распределение**.
3. Выберите несколько каналов и CDN.
4. Нажмите **Назначить CDN** — это пока не меняет Flussonic.
5. Нажмите **Применить** без галочки удаления: целевая копия будет создана и проверена, старые останутся.
6. После проверки снова нажмите **Применить** с галочкой **удалить лишние копии**.

При выключенной галочке **Гибридное распределение** панель полностью работает в зеркальной модели. Сохранённые назначения не удаляются и снова активируются после включения.

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

Не меняйте `PANEL_SECRET_KEY`: иначе сохранённые пароли и `cluster_key` не расшифруются. Не удаляйте Docker volume `flussonic-panel-data`: в нём находятся настройки размещения, история и резервные копии.
