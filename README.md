# Cyrius Stream Control v5.9.3

Тёмная админ-панель для управления несколькими Flussonic Media Server.

## Новое в v5.9.3

- быстрый переключатель `On demand / Static` для каждого потока;
- массовые кнопки `On demand` и `Static` для отмеченных потоков;
- выбор режима по умолчанию прямо в M3U-импорте;
- переключение учитывает гибридное размещение: зеркало меняется на всех CDN, назначенный поток — только на своём CDN;
- перед сменой режима сохраняется backup, отсутствующие потоки не создаются автоматически.


### Массовый импорт M3U

- Вставка обычного M3U/M3U8 текста прямо в панели.
- Используются только отображаемое название канала из `#EXTINF` и следующая строка URL.
- `tvg-id`, `tvg-logo`, `group-title`, `tvg-chno` и другие атрибуты игнорируются.
- Автоматически генерируется безопасное системное имя Flussonic, включая транслитерацию кириллицы.
- Предпросмотр перед записью: название, системное имя, URL и отметка уже существующих потоков.
- Повторы внутри одного M3U пропускаются.
- Уже существующие потоки по умолчанию не перезаписываются; обновление включается отдельной галочкой.
- При импорте можно выбрать зеркальную модель или назначить все импортируемые каналы на один CDN.
- Импорт выполняется только по запросу пользователя и ограничивает параллелизм, чтобы не создавать всплеск запросов к старым Flussonic.

## Новое в v5.9

### Гибридное размещение

- Общий переключатель между зеркальной и гибридной моделью.
- Постепенная миграция: неназначенные каналы остаются зеркальными.
- Массовое назначение выбранных каналов на конкретный CDN.
- Отдельное безопасное применение назначения.
- Удаление старых копий только по явной галочке и только после проверки целевого CDN.
- Счётчик назначений по серверам с ориентиром 100 каналов на CDN.

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

## Ручная проверка источников

Панель не создаёт фоновую задачу проверки input. Открытие раздела **Источники** только читает последние сохранённые результаты из базы. Сетевые запросы к HLS/HTTP/M4F выполняются исключительно после нажатия **Проверить все сейчас** или **Проверить** возле конкретного потока.

Старые параметры `SOURCE_CHECK_ENABLED` и `SOURCE_CHECK_SECONDS` сохранены в `.env.example` для совместимости, но не включают планировщик.

## Возможности

- управление Flussonic-серверами;
- создание, редактирование, удаление и перестановка потоков;
- drag-and-drop input;
- ручная проверка всех источников или отдельного потока без фонового обхода;
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

## Обновление с предыдущей версии без rsync

Архив загрузите как `/root/flussonic-panel-v5.9.3.zip`, затем запустите поставляемый скрипт:

```bash
rm -rf /tmp/flussonic-panel-v593-installer
mkdir -p /tmp/flussonic-panel-v593-installer
unzip -q -o /root/flussonic-panel-v5.9.3.zip -d /tmp/flussonic-panel-v593-installer
bash /tmp/flussonic-panel-v593-installer/flussonic-panel/upgrade-from-zip.sh
```

Скрипт сохраняет `.git` и `.env`, не использует `rsync`, пересобирает отдельный образ `flussonic-panel:v593` и оставляет Docker volume `flussonic-panel-data` без изменений.

Не удаляйте volume и не меняйте `PANEL_SECRET_KEY`.

## Проверка

```bash
curl -s http://127.0.0.1:8088/api/health
```

Ожидается:

```json
{"ok":true,"version":"5.9.3","cluster":true,"placement":true}
```

Откройте:

```text
http://IP_ПАНЕЛИ:8088/?v=592
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
