## ADDED Requirements

### Requirement: Standalone asynchronous stream application
Приложение SHALL иметь отдельный пакет и окружение, принимать HTTP NDJSON и передавать
записи в PostgreSQL асинхронно, без накопления всего запроса в памяти.

#### Scenario: Stream larger than memory buffer
- **WHEN** клиент отправляет допустимый поток несколькими порциями
- **THEN** сервис передаёт записи в COPY по мере чтения и соблюдает заданные лимиты

### Requirement: Validate records and bound resources
Сервис SHALL принимать только JSON object на строку, отвергать неоднозначный JSON
и неподдерживаемые JSONB значения, ограничивать размер потока/строки и время ожидания.
Сервис SHALL ограничивать число занятых соединений размером пула.

#### Scenario: Invalid record after valid prefix
- **WHEN** после корректных строк приходит пустая строка, неверный JSON, повтор ключа или скаляр
- **THEN** загрузка завершается ошибкой без подтверждённого частичного результата

#### Scenario: Interrupted or oversized input
- **WHEN** поток оборван, превысил лимит или истёк timeout
- **THEN** сервис прекращает загрузку и отменяет её транзакцию

### Requirement: Atomic COPY and receipt
Сервис SHALL сохранять все строки и receipt одной транзакцией в фиксированной схеме.
Успешный ответ MUST отправляться только после подтверждённого commit.

#### Scenario: Successful upload
- **WHEN** корректный поток нового UUID полностью записан и commit подтверждён
- **THEN** строки и receipt становятся доступны вместе

#### Scenario: Database failure during COPY
- **WHEN** запись завершается ошибкой до commit
- **THEN** ни receipt, ни частичный набор строк не считаются опубликованными

### Requirement: Idempotent identity and conflict detection
Сервис SHALL сериализовать загрузки одного UUID и сравнивать SHA-256 исходных байтов.
Повтор совпавшей загрузки SHALL возвращать существующий receipt без новых строк.

#### Scenario: Same UUID and exact same bytes
- **WHEN** клиент повторяет подтверждённую загрузку с тем же UUID и байтами
- **THEN** сервис возвращает существующий результат и не дублирует строки

#### Scenario: Same UUID and changed bytes
- **WHEN** клиент использует подтверждённый UUID с другим содержимым
- **THEN** сервис возвращает конфликт и сохраняет исходный результат

### Requirement: Recovery after unknown commit outcome
Сервис SHALL предоставлять GET receipt по UUID. Ошибка ответа на PUT MUST NOT требовать
нового UUID или слепой повторной вставки.

#### Scenario: Commit succeeded but HTTP response was lost
- **WHEN** отправитель запрашивает receipt исходного UUID
- **THEN** сервис возвращает подтверждённый результат

### Requirement: Explicit initialization and authenticated API
Инициализация схемы SHALL быть отдельной командой. Рабочий сервер SHALL NOT выполнять DDL.
PUT, GET receipt и readiness SHALL требовать Bearer token; liveness SHALL быть публичным.

#### Scenario: Request without valid token
- **WHEN** клиент обращается к защищённому endpoint без корректного token
- **THEN** запрос отклоняется до обработки данных

#### Scenario: Start server on existing database
- **WHEN** запускается serve
- **THEN** сервис подключается к настроенной БД без автоматического создания базы и таблиц
