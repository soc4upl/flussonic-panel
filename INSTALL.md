# Cyrius Stream Control — инструкция по установке

Cyrius Stream Control — панель управления потоками и серверами Flussonic.

Панель запускается в Docker и хранит настройки, историю, резервные копии и результаты мониторинга в отдельном Docker volume.

## Системные требования

Рекомендуемая система:

* Ubuntu 20.04, 22.04 или 24.04;
* Debian 11 или 12;
* минимум 2 CPU;
* минимум 2 GB RAM;
* установленный Docker;
* доступ к Flussonic API;
* открытый порт панели, по умолчанию `8088`.

## 1. Установка необходимых пакетов

```bash
apt-get update
apt-get install -y git docker.io curl
```

Запустите Docker:

```bash
systemctl enable --now docker
```

Проверьте:

```bash
docker --version
systemctl status docker --no-pager
```

## 2. Настройка доступа к приватному GitHub-репозиторию

Создайте SSH-ключ:

```bash
mkdir -p /root/.ssh
chmod 700 /root/.ssh

ssh-keygen -t ed25519 \
  -C "flussonic-panel-deploy" \
  -f /root/.ssh/flussonic-panel-deploy \
  -N ""
```

Покажите публичный ключ:

```bash
cat /root/.ssh/flussonic-panel-deploy.pub
```

Добавьте его в GitHub:

```text
GitHub repository
→ Settings
→ Deploy keys
→ Add deploy key
```

Вставьте публичный ключ. Разрешение `Allow write access` для обычной установки не требуется.

Создайте SSH-конфигурацию:

```bash
cat > /root/.ssh/config <<'EOF'
Host github-flussonic
    HostName github.com
    User git
    IdentityFile /root/.ssh/flussonic-panel-deploy
    IdentitiesOnly yes
EOF
```

Установите права:

```bash
chmod 600 /root/.ssh/config
chmod 600 /root/.ssh/flussonic-panel-deploy
chmod 644 /root/.ssh/flussonic-panel-deploy.pub
```

Проверьте подключение:

```bash
ssh -T git@github-flussonic
```

При успешном подключении GitHub покажет сообщение:

```text
You've successfully authenticated, but GitHub does not provide shell access.
```

## 3. Клонирование панели

```bash
git clone \
  git@github-flussonic:soc4upl/flussonic-panel.git \
  /root/flussonic-panel
```

Перейдите в папку:

```bash
cd /root/flussonic-panel
```

## 4. Создание файла настроек

Если в проекте есть пример `.env.example`:

```bash
cp .env.example .env
```

Откройте файл:

```bash
nano .env
```

Минимальный пример:

```env
PANEL_SECRET_KEY=ЗАМЕНИТЕ_НА_ДЛИННЫЙ_СЛУЧАЙНЫЙ_КЛЮЧ

SERVER_LOAD_ENABLED=true
SERVER_LOAD_POLL_SECONDS=60
SERVER_NETWORK_POLL_SECONDS=1
SERVER_LOAD_HISTORY_SECONDS=60

SERVER_LOAD_CPU_WARNING=85
SERVER_LOAD_MEMORY_WARNING=85
SERVER_LOAD_DISK_WARNING=90
```

Создать случайный секретный ключ можно командой:

```bash
openssl rand -hex 32
```

Установите безопасные права:

```bash
chmod 600 .env
```

Важно: `PANEL_SECRET_KEY` используется для шифрования сохранённых паролей серверов. Не меняйте этот ключ после настройки панели.

## 5. Первый запуск

```bash
cd /root/flussonic-panel

chmod +x run-docker.sh
./run-docker.sh
```

Проверьте контейнер:

```bash
docker ps --filter name=flussonic-panel
```

Посмотрите журнал:

```bash
docker logs --tail=100 flussonic-panel
```

Проверьте API панели:

```bash
curl -s http://127.0.0.1:8088/api/health
```

Ожидается ответ примерно такого вида:

```json
{
  "ok": true,
  "version": "5.7.0"
}
```

## 6. Открытие панели

Откройте в браузере:

```text
http://IP_СЕРВЕРА:8088
```

После обновления используйте адрес с версией для очистки браузерного кэша:

```text
http://IP_СЕРВЕРА:8088/?v=57
```

Также можно нажать:

```text
Ctrl + Shift + R
```

## 7. Добавление Flussonic-сервера

Откройте:

```text
Серверы → Добавить сервер
```

Заполните:

* название сервера;
* URL Flussonic;
* логин;
* пароль;
* Node Exporter URL;
* проверку TLS-сертификата.

Пример:

```text
Название: CDN 2
Flussonic URL: http://192.0.2.10:8080
Node Exporter URL: http://192.0.2.10:9100/metrics
```

Нажмите `Проверить подключение`, затем сохраните сервер.

## 8. Установка Node Exporter

Для мониторинга CPU, RAM, дисков и сети установите Node Exporter на каждом Flussonic-сервере:

```bash
apt-get update
apt-get install -y prometheus-node-exporter
```

Запустите:

```bash
systemctl enable --now prometheus-node-exporter
```

Проверьте:

```bash
curl -s http://127.0.0.1:9100/metrics | head
```

Разрешите подключение к порту `9100` только с IP сервера панели:

```bash
ufw allow from IP_СЕРВЕРА_ПАНЕЛИ to any port 9100 proto tcp
ufw reload
```

Не открывайте Node Exporter для всего интернета.

Проверка с сервера панели:

```bash
curl -sS --max-time 5 \
  http://IP_FLUSSONIC_СЕРВЕРА:9100/metrics \
  | head
```

## 9. Обновление панели

На сервере панели выполните:

```bash
cd /root/flussonic-panel
git pull --ff-only
./run-docker.sh
```

Проверьте новую версию:

```bash
curl -s http://127.0.0.1:8088/api/health
```

Docker volume с настройками и историей при обновлении не удаляется.

## 10. Создание команды быстрого обновления

```bash
cat > /usr/local/bin/update-flussonic-panel <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail

cd /root/flussonic-panel

echo "Получение обновлений..."
git pull --ff-only

echo "Перезапуск панели..."
./run-docker.sh

echo "Проверка..."
sleep 3
curl -fsS http://127.0.0.1:8088/api/health
echo
EOF
```

Установите права:

```bash
chmod +x /usr/local/bin/update-flussonic-panel
```

Теперь обновление запускается одной командой:

```bash
update-flussonic-panel
```

## 11. Резервное копирование данных

Код хранится в GitHub, но настройки и история находятся в Docker volume:

```text
flussonic-panel-data
```

Создайте резервную копию:

```bash
docker run --rm \
  -v flussonic-panel-data:/data:ro \
  -v /root:/backup \
  alpine \
  tar czf /backup/flussonic-panel-data.tar.gz -C /data .
```

Сохраните `.env`:

```bash
cp /root/flussonic-panel/.env \
  /root/flussonic-panel.env.backup

chmod 600 /root/flussonic-panel.env.backup
```

Для полного восстановления нужны два файла:

```text
/root/flussonic-panel.env.backup
/root/flussonic-panel-data.tar.gz
```

## 12. Восстановление на новом сервере

Клонируйте проект:

```bash
git clone \
  git@github-flussonic:soc4upl/flussonic-panel.git \
  /root/flussonic-panel
```

Восстановите `.env`:

```bash
cp /root/flussonic-panel.env.backup \
  /root/flussonic-panel/.env

chmod 600 /root/flussonic-panel/.env
```

Создайте Docker volume:

```bash
docker volume create flussonic-panel-data
```

Восстановите данные:

```bash
docker run --rm \
  -v flussonic-panel-data:/data \
  -v /root:/backup \
  alpine \
  tar xzf /backup/flussonic-panel-data.tar.gz -C /data
```

Запустите панель:

```bash
cd /root/flussonic-panel
chmod +x run-docker.sh
./run-docker.sh
```

## 13. Полезные команды

Статус контейнера:

```bash
docker ps --filter name=flussonic-panel
```

Журнал панели:

```bash
docker logs --tail=200 flussonic-panel
```

Просмотр журнала в реальном времени:

```bash
docker logs -f flussonic-panel
```

Перезапуск:

```bash
docker restart flussonic-panel
```

Остановка:

```bash
docker stop flussonic-panel
```

Запуск:

```bash
docker start flussonic-panel
```

Проверка занятого порта:

```bash
ss -lntp | grep 8088
```

Проверка Docker volume:

```bash
docker volume inspect flussonic-panel-data
```

## 14. Решение распространённых проблем

### Панель не открывается

Проверьте контейнер:

```bash
docker ps -a --filter name=flussonic-panel
docker logs --tail=200 flussonic-panel
```

Проверьте порт:

```bash
curl -v http://127.0.0.1:8088/api/health
ss -lntp | grep 8088
```

### Permission denied при Git clone

Проверьте SSH:

```bash
ssh -T git@github-flussonic
```

Проверьте используемый ключ:

```bash
ssh -G github-flussonic | grep -i identityfile
```

### Node Exporter недоступен

На Flussonic-сервере:

```bash
systemctl status prometheus-node-exporter --no-pager
ss -lntp | grep 9100
curl -s http://127.0.0.1:9100/metrics | head
```

На сервере панели:

```bash
curl -sS --max-time 5 \
  http://IP_FLUSSONIC_СЕРВЕРА:9100/metrics \
  | head
```

### После обновления старый интерфейс

Откройте панель с параметром версии:

```text
http://IP_СЕРВЕРА:8088/?v=57
```

Или выполните жёсткое обновление страницы:

```text
Ctrl + Shift + R
```

## Безопасность

* Не добавляйте `.env` в GitHub.
* Не публикуйте пароли Flussonic.
* Не публикуйте Telegram Bot Token.
* Не меняйте `PANEL_SECRET_KEY` после сохранения серверов.
* Ограничьте доступ к порту `9100`.
* Разрешите доступ к панели только с доверенных IP или через VPN.
* Регулярно создавайте резервную копию Docker volume.
