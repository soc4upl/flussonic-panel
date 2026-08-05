# Установка Cyrius Stream Control v5.13.1

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

Ожидаемая версия: `5.13.1`. Панель: `http://IP_СЕРВЕРА:8088/?v=5130`.

## Обновление из GitHub

```bash
cd /root/flussonic-panel
git pull --ff-only
./run-docker.sh
```

## Обновление из ZIP без rsync

Архив должен находиться в `/root/flussonic-panel-v5.13.1.zip`.

```bash
rm -rf /tmp/flussonic-panel-v5131
mkdir -p /tmp/flussonic-panel-v5131
unzip -q -o /root/flussonic-panel-v5.13.1.zip -d /tmp/flussonic-panel-v5131

cp /root/flussonic-panel/.env /root/flussonic-panel.env.backup
docker rm -f flussonic-panel 2>/dev/null || true

cd /root/flussonic-panel
find . -mindepth 1 -maxdepth 1 ! -name '.git' ! -name '.env' -exec rm -rf -- {} +
cp -a /tmp/flussonic-panel-v5131/flussonic-panel/. /root/flussonic-panel/
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

Образ должен быть `flussonic-panel:v5131`.

## Первое включение распределения

1. Откройте **Настройки** и выберите **Гибридный** режим. После этого появится раздел **Размещение**.
2. Для первого теста выберите 2–3 неважных канала и целевой CDN.
3. Нажмите **Перенести сейчас** и внимательно проверьте Dry Run.
4. После применения откройте эти каналы через тот же LB URL, которым пользуются клиенты. Убедитесь, что просмотр работает, а upstream открывает только назначенный CDN.
5. После успешного теста используйте **План 100/CDN**. Размер пачки можно менять.
6. Нажмите **Выбрать эту пачку**, затем **Перенести сейчас**. Панель проверит target, сохранит backup, удалит лишние копии и зафиксирует Assigned.
7. Не мигрируйте большую пачку, если любой CDN отображается недоступным: backend специально блокирует удаление при неполной проверке.

Кнопка **Только назначить** по-прежнему меняет лишь желаемое состояние и не удаляет копии. Старый режим **Применить назначение** также сохранён для ручного двухэтапного сценария.

В режиме **Зеркальный** панель работает в зеркальной модели. Сохранённые назначения не удаляются и снова активируются после выбора **Гибридный**.

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
