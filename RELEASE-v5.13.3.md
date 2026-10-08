# Cyrius Stream Control v5.13.3

Hotfix для Flussonic 23: массовый Static / On demand теперь сохраняется через полный `config_on_disk` и подтверждается повторным GET перед успешным ответом.

- quick/bulk Static / On demand сохраняет полный disk config;
- `config_on_disk.static` обязательно проверяется после PUT;
- при несовпадении операция возвращает ошибку и не считается успешной;
- старый массовый диалог `set_static` использует ту же безопасную схему;
- inputs/title/provider/on_play и остальные поля сохраняются при смене режима.
