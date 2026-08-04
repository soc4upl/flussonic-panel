# Cyrius Stream Control v5.10.0

## Новое

- Центр изменений / Dry Run для массовых операций.
- Dry Run для переключения On demand / Static.
- Dry Run для временного отключения / включения потока.
- Точный diff по потоку и CDN: поле, старое значение, новое значение.
- Dry Run не выполняет PUT/DELETE и не создаёт backup.
- Карточка канала `▣` со сводкой по всем CDN.
- В карточке: размещение, status, clients, input/output bitrate, inputs, Audit и последние backup.
- Быстрый переход из карточки в Preview, Inputs, Диагностику и Редактирование.
- Проверка источников остаётся только ручной.

## Совместимость

Обновляется поверх v5.9.4 с сохранением `.git`, `.env` и Docker volume `flussonic-panel-data`.
