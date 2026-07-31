# Cyrius Stream Control v5.8

Тёмная админ-панель для управления несколькими Flussonic Media Server.

## Новое в v5.8

### Раздел Cluster

Панель поддерживает отдельную конфигурацию load balancer и peer-серверов:

- зашифрованное хранение `cluster_key`;
- выбор Flussonic-сервера, который выполняет роль load balancer;
- режимы `clients`, `bitrate`, `usage` и `streams`;
- список peer с отдельным hostname;
- необязательный `max_bitrate` для каждого peer;
- генерация готового фрагмента `flussonic.conf`;
- таблица `Host`, `CPU`, `Mem`, `Clients`, `Streams`, `Output bitrate`, `Load`, `Uptime`.

Источники метрик:

- CPU, RAM, load average и uptime — Node Exporter;
- Clients — Flussonic sessions API;
- Streams и output bitrate — Flussonic streams API.

Поэтому Cluster работает со старыми версиями Flussonic, у которых нет современного runtime metrics API.

> Панель не перезаписывает `flussonic.conf` автоматически. Для старых установок конфигурация генерируется для ручного применения после резервной копии.

### Исправление умной синхронизации

Сравнение больше не считает различиями:

- `position`;
- runtime-статистику;
- `named_by` и другие служебные поля;
- разное представление `input` и `on_play` в старых и новых API.

Для настоящих отличий рядом с сервером отображаются конкретные поля, например `inputs`, `provider` или `on_play`.

## Возможности

- управление Flussonic-серверами;
- создание, редактирование, удаление и перестановка потоков;
- drag-and-drop input;
- фильтр и проверка нерабочих источников;
- быстрые действия для input;
- резервные копии и откат;
- история действий;
- массовые операции;
- умная синхронизация;
- Telegram/webhook-уведомления;
- preview и диагностика;
- мониторинг сессий;
- CPU, RAM, диски и RX/TX через Node Exporter;
- Cluster dashboard.

## Обновление с предыдущей версии

```bash
cd /root

cp /root/flussonic-panel/.env \
   /root/flussonic-panel.env.backup

rm -rf /root/flussonic-panel
unzip /root/flussonic-panel-v5.8.zip -d /root

cp /root/flussonic-panel.env.backup \
   /root/flussonic-panel/.env

cd /root/flussonic-panel
chmod +x run-docker.sh
./run-docker.sh
```

Docker volume `flussonic-panel-data` сохраняет серверы, пароли, историю, backups, метрики и Cluster settings.

Не удаляйте volume и не меняйте `PANEL_SECRET_KEY`.

## Проверка

```bash
curl -s http://127.0.0.1:8088/api/health
```

Ожидается:

```json
{"ok":true,"version":"5.8.0","cluster":true}
```

Откройте:

```text
http://IP_ПАНЕЛИ:8088/?v=58
```

## Настройка Cluster

1. Добавьте load balancer и CDN-узлы в разделе **Серверы**.
2. Для CDN-узлов настройте Node Exporter URL.
3. Откройте **Cluster → Настроить**.
4. Выберите balancer-сервер.
5. Укажите имя balancer, режим и `cluster_key`.
6. Отметьте peer-серверы и проверьте их hostname.
7. Сохраните и нажмите **Показать конфиг**.
8. Сделайте backup `flussonic.conf`, примените конфиг вручную и перезапустите Flussonic.

Пример генерируемого конфига:

```text
cluster_key CHANGE_ME;

peer cdn-1.example.com {
}
peer cdn-2.example.com {
}

balancer lb01 {
  mode clients;
  server cdn-1.example.com;
  server cdn-2.example.com;
}
```

## Node Exporter

На каждом Flussonic-сервере:

```bash
apt-get update
apt-get install -y prometheus-node-exporter
systemctl enable --now prometheus-node-exporter
```

Разрешите порт только IP панели:

```bash
ufw allow from IP_СЕРВЕРА_ПАНЕЛИ to any port 9100 proto tcp
```

## Переменные окружения

```env
PANEL_CLUSTER_STORE=/app/data/cluster.json
CLUSTER_POLL_SECONDS=15

SERVER_LOAD_ENABLED=true
SERVER_LOAD_POLL_SECONDS=60
SERVER_NETWORK_POLL_SECONDS=1
```

## Логи

```bash
docker ps --filter name=flussonic-panel
docker logs --tail=200 flussonic-panel
```
