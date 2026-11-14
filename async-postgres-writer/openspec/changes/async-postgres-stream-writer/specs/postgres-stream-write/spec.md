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
PUT, GET receipt, GET данных и readiness SHALL требовать Bearer token; liveness SHALL быть публичным.

#### Scenario: Request without valid token
- **WHEN** клиент обращается к защищённому endpoint без корректного token
- **THEN** запрос отклоняется до обработки данных

#### Scenario: Start server on existing database
- **WHEN** запускается serve
- **THEN** сервис подключается к настроенной БД без автоматического создания базы и таблиц

### Requirement: Read committed upload data by page or stream
Сервис SHALL предоставлять чтение строк одной подтверждённой загрузки по UUID с
сортировкой по возрастанию `row_number`. Для неподтверждённого или неизвестного
UUID сервис SHALL возвращать `404`. API SHALL использовать существующую схему и
помечать ответы с данными заголовком `Cache-Control: no-store`.

#### Scenario: Read a page of rows
- **WHEN** клиент запрашивает `GET /v1/uploads/{upload_id}/rows` с `limit` от 1 до 1000 и `offset` от 0 до `9223372036854775807`
- **THEN** сервис возвращает JSON с `upload_id`, общим `row_count`, `limit`, `offset` и массивом `rows`, где каждая запись содержит `row_number` и `data`
- **AND** при отсутствии параметров используются `limit=100` и `offset=0`
- **AND** при смещении за пределами результата сервис возвращает `200` с пустым массивом

#### Scenario: Reject invalid pagination
- **WHEN** `limit`, `offset` или `after_row_number` не являются целыми числами допустимого диапазона либо клиент одновременно передаёт `after_row_number` и `offset` (включая `offset=0`)
- **THEN** сервис возвращает `422`

#### Scenario: Read rows after a known row number
- **WHEN** клиент запрашивает страницу с `after_row_number` от 0 до `9223372036854775807` и без `offset`
- **THEN** сервис возвращает упорядоченные строки с `row_number > after_row_number` через диапазон существующего ключа `(upload_id, row_number)`
- **AND** JSON содержит `upload_id`, `row_count`, `limit`, `rows`, `after_row_number` и `next_after_row_number`: номер последней строки страницы, если есть следующая страница, иначе `null`
- **AND** запросы без `after_row_number` сохраняют формат ответа с `offset`

#### Scenario: Reject an oversized page
- **WHEN** сериализованное тело JSON-страницы превышает 16 МиБ
- **THEN** сервис возвращает `413` и предлагает уменьшить `limit` или использовать поток

#### Scenario: Stream an upload with bounded resources
- **WHEN** клиент запрашивает `GET /v1/uploads/{upload_id}/rows/stream`
- **THEN** сервис отдаёт `application/x-ndjson` с `X-Row-Count`, по одному объекту с `row_number` и `data` на строку, без накопления всего результата в памяти
- **AND** сервис отправляет тело порциями не более 128 КиБ, сохраняя содержимое и порядок строк, даже если отдельная строка разбита между порциями
- **AND** сервис ограничивает общее время потоковой выгрузки, включая передачу ответа, и освобождает соединение при её завершении или прерывании

#### Scenario: Database fails before streaming begins
- **WHEN** ошибка БД произошла до отправки заголовков
- **THEN** сервис возвращает `503`

#### Scenario: Database fails after streaming begins
- **WHEN** ошибка БД произошла после отправки заголовков потокового ответа
- **THEN** сервис прерывает ответ без успешного завершения потока
