# Cyrius Stream Control v5.12.0

## Migration Rollback

Перед запуском `/api/placement/migrate` панель сохраняет снимок каждой выбранной трансляции: исходное размещение и `config_on_disk` на каждом включённом CDN.

Новые API:

- `GET /api/placement/migrations` — история миграций;
- `POST /api/placement/migrations/{id}/rollback/dry-run` — безопасный предпросмотр;
- `POST /api/placement/migrations/{id}/rollback` — восстановление состояния до миграции.

Rollback сначала проверяет доступность всех CDN из снимка. Перед перезаписью/удалением создаётся обычный backup. Миграция помечается откатанной только после полного успеха.
