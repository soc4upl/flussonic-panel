# Cyrius Stream Control v5.7

Единая тёмная админ-панель для управления несколькими Flussonic Media Server.

## Новое в v5.6 — исправленный график и RX/TX в реальном времени

Системные метрики старых версий Flussonic теперь читаются через Linux `node_exporter`.

RX/TX теперь обновляются отдельным лёгким циклом раз в секунду. Этот цикл запрашивает только сетевой collector Node Exporter и не опрашивает Flussonic API. CPU, RAM и диски продолжают обновляться с безопасным интервалом `SERVER_LOAD_POLL_SECONDS`. График исправлен: canvas занимает всю доступную высоту, показывает точку уже после первого замера и строит линии с секундными метками в режиме Live.


Панель показывает:

- загрузку CPU;
- использование RAM;
- заполнение дисков;
- системный load average;
- uptime Linux;
- текущий входящий трафик RX;
- текущий исходящий трафик TX;
- общий RX+TX на графике;
- сетевую нагрузку отдельно по каждому интерфейсу (`eth0`, `ens3` и т. д.);
- накопительный объём полученных и отправленных данных;
- состояние интерфейса UP/DOWN;
- историю за Live, 1 час, 24 часа, 7 и 30 дней.

Node Exporter является основным источником системных метрик. Если он недоступен, панель пробует runtime metrics Flussonic, а затем использует ограниченный fallback по статистике потоков.

## Настройка Node Exporter

На каждом Flussonic/Linux-сервере:

```bash
apt-get update
apt-get install -y prometheus-node-exporter
systemctl enable --now prometheus-node-exporter
```

Локальная проверка:

```bash
curl -s http://127.0.0.1:9100/metrics | head
```

Разрешите порт `9100` только IP сервера панели:

```bash
ufw allow from IP_СЕРВЕРА_ПАНЕЛИ to any port 9100 proto tcp
```

Проверка с сервера панели:

```bash
curl -sS --max-time 5 http://IP_FLUSSONIC:9100/metrics | head
```

В панели откройте:

```text
Серверы → Редактировать сервер → Node Exporter URL
```

Можно указать:

```text
http://cdn-2.example.com:9100/metrics
```

Поле можно оставить пустым. Тогда панель автоматически попробует:

```text
http://HOST_ИЗ_FLUSSONIC_URL:9100/metrics
```

Кнопка «Проверить подключение» проверяет отдельно Flussonic API и Node Exporter.

## Возможности панели

- Добавление и редактирование Flussonic-серверов из панели.
- Создание, редактирование, удаление и перестановка потоков.
- Drag-and-drop приоритета input.
- Фильтр нерабочих источников.
- Проверка `hls://`, `hlss://`, `tshttp://`, `tshttps://`, `m4f://`, `m4fs://`.
- Быстрые действия с проблемными input.
- Резервные копии и откат.
- История действий.
- Массовые операции.
- Умная синхронизация серверов.
- Telegram/webhook-уведомления.
- Встроенный preview и диагностика потока.
- Мониторинг сессий и история за 1 час, 24 часа, 7 и 30 дней.
- Мониторинг CPU, RAM, дисков и сети через Node Exporter.

## Обновление с предыдущей версии

Загрузите `flussonic-panel-v5.7.zip` в `/root`, затем:

```bash
cd /root

cp /root/flussonic-panel/.env \
   /root/flussonic-panel.env.backup

rm -rf /root/flussonic-panel
unzip /root/flussonic-panel-v5.7.zip -d /root

cp /root/flussonic-panel.env.backup \
   /root/flussonic-panel/.env

cd /root/flussonic-panel
chmod +x run-docker.sh
./run-docker.sh
```

Список серверов, зашифрованные пароли, backups, история и метрики сохранятся в Docker volume `flussonic-panel-data`.

Не удаляйте volume и не меняйте старый `PANEL_SECRET_KEY`.

## Проверка версии

```bash
curl -s http://127.0.0.1:8088/api/health
```

Ожидаемый ответ:

```json
{"ok":true,"version":"5.7.0","server_load_monitor":true,"node_exporter":true}
```

Откройте панель с очисткой кэша:

```text
http://IP_СЕРВЕРА:8088/?v=57
```

Или нажмите `Ctrl + Shift + R`.

## Настройки мониторинга нагрузки

```env
SERVER_LOAD_ENABLED=true
SERVER_LOAD_POLL_SECONDS=60
SERVER_NETWORK_POLL_SECONDS=1
SERVER_LOAD_HISTORY_SECONDS=60
SERVER_LOAD_CPU_WARNING=85
SERVER_LOAD_MEMORY_WARNING=85
SERVER_LOAD_DISK_WARNING=90
```

После изменения `.env` пересоздайте контейнер:

```bash
cd /root/flussonic-panel
./run-docker.sh
```

RX/TX начинают отображаться после второго секундного замера, потому что скорость вычисляется по разнице накопительных счётчиков. CPU, RAM и диски обновляются отдельно с интервалом `SERVER_LOAD_POLL_SECONDS`.

## Логи

```bash
docker ps --filter name=flussonic-panel
docker logs --tail=200 flussonic-panel
```

## Исправление v5.7 — очистка удалённых источников

После успешной проверки панель теперь сравнивает сохранённые результаты с актуальной конфигурацией Flussonic. Если input был удалён через панель или напрямую во Flussonic, его устаревшая запись удаляется из SQLite и больше не отображается в разделе «Источники».

Очистка выполняется только после успешного получения конфигурации конкретного сервера. При timeout, HTTP-ошибке или недоступности Flussonic история не удаляется.

Результат ручной проверки содержит поле `removed_stale`, а уведомление в интерфейсе показывает количество очищенных записей.
