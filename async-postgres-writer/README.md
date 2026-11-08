# Async PostgreSQL Writer

Отдельное async-приложение принимает HTTP-поток NDJSON и записывает строки в PostgreSQL
через `asyncpg` и `COPY`. Весь результат и receipt фиксируются одной транзакцией.
Приложение не связано с Kafka worker или Hadoop writer и имеет собственные зависимости.

## Контракт

```text
PUT /v1/uploads/{upload_id}   NDJSON → COPY → COMMIT → receipt
GET /v1/uploads/{upload_id}   проверка результата после потери ответа
```

`upload_id` — UUID, выбранный отправителем до начала загрузки. Для обоих запросов нужен
`Authorization: Bearer <WRITER_API_TOKEN>`. PUT принимает только
`Content-Type: application/x-ndjson`.
Сжатое тело (`Content-Encoding: gzip` и другие варианты) не поддерживается.

Каждая строка — JSON object в UTF-8. Последний перевод строки необязателен. Пустые строки,
массивы/скаляры вместо объекта, повторяющиеся ключи, NaN/Infinity и значения, которые
PostgreSQL JSONB не может представить, отклоняются. Ошибка в конце потока отменяет всю
загрузку, включая уже переданные через COPY строки.

Пример входа:

```json
{"calculation_id":"calc-42","value":12.5}
{"calculation_id":"calc-42","value":null}
```

Пока предметная схема не определена, данные хранятся как JSONB. Это ограниченный
контракт приёма JSON, а не проверка полей результата калькуляции. Таблица и SQL не
выбираются пользователем запроса.

## Запуск локально

Требуются Python 3.12+, `uv` и доступный PostgreSQL. Зависимости и точные версии находятся
в `pyproject.toml` и `uv.lock`; PostgreSQL-драйвер — `asyncpg`, HTTP-приложение — FastAPI.

```bash
cd async-postgres-writer
uv sync --locked
export WRITER_DATABASE_URL='postgresql://writer:password@localhost:5432/stream_writer'
export WRITER_API_TOKEN='local-demo-token-change-before-deploy-1234567890'
uv run --locked async-postgres-writer init-db
uv run --locked async-postgres-writer serve --host 127.0.0.1 --port 8080
```

`init-db` явно создаёт фиксированную схему и таблицы в **уже существующей базе**.
`serve` не создаёт базу и не выполняет миграции. Для эксплуатации выполните init от
отдельной роли с DDL-правами, а runtime дайте права на чтение и запись созданных таблиц.
Повторный init не заменяет существующие данные и не является механизмом изменения схемы.

После создания runtime-роли владелец схемы выдаёт ей минимальные права:

```sql
GRANT USAGE ON SCHEMA stream_writer TO writer_runtime;
GRANT SELECT, INSERT, UPDATE ON stream_writer.uploads TO writer_runtime;
GRANT SELECT, INSERT ON stream_writer.rows TO writer_runtime;
```

Создайте файл `rows.ndjson` с приведёнными выше строками и отправьте его без буферизации
полного тела на стороне сервиса:

```bash
upload_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
curl --fail-with-body -X PUT "http://127.0.0.1:8080/v1/uploads/$upload_id" \
  -H "Authorization: Bearer $WRITER_API_TOKEN" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @rows.ndjson

curl --fail-with-body "http://127.0.0.1:8080/v1/uploads/$upload_id" \
  -H "Authorization: Bearer $WRITER_API_TOKEN"
```

Сохраните UUID вместе с исходным файлом до запроса. Receipt содержит идентификатор,
SHA-256 исходных байтов и счётчики строк/байтов. GET возвращает только подтверждённую
загрузку; отсутствие receipt не доказывает, что параллельная загрузка уже завершилась.
`committed_at` записывается перед commit и возвращается только после его подтверждения;
это время подготовки receipt, а не серверный журнал времени commit.

```json
{
  "upload_id": "00000000-0000-0000-0000-000000000001",
  "sha256": "<SHA-256 исходных байтов>",
  "row_count": 2,
  "byte_count": 91,
  "committed_at": "2026-11-08T10:00:00Z",
  "reused": false
}
```

Новый результат возвращает `201`, совпавший повтор — `200`, неизвестный UUID в GET —
`404`. Некорректный поток даёт `400`, превышение размера — `413`, неверный media type —
`415`, timeout входа — `408`; ошибки БД — `503`. `409` также означает, что загрузка того
же UUID уже выполняется: дождитесь её результата и выполните GET или повтор.

## Повторы и надёжность

- Новый UUID и корректный поток создают один результат; ответ отправляется после commit.
- Повтор с тем же UUID и **теми же исходными байтами** возвращает прежний результат.
  Дубликаты строк не добавляются. Для проверки повтора сервис должен прочитать весь поток.
- Тот же UUID с другими байтами возвращает `409`. Пробелы, порядок ключей и последний
  перевод строки участвуют в SHA-256, даже если JSON имеет тот же смысл.
- При ошибке парсинга, превышении лимита, обрыве потока или ошибке COPY транзакция
  отменяется. Частичный результат не становится видимым читателю.
- Если соединение оборвалось во время commit или потерялся HTTP-ответ, сначала выполните
  GET с исходным UUID. При необходимости повторите исходный поток с тем же UUID.
  Новый UUID при таком сбое может создать отдельную копию результата.

Данные лежат в `stream_writer.rows` (`upload_id`, `row_number`, `data jsonb`), receipt —
в `stream_writer.uploads`. Читатель должен выбирать строки по подтверждённому UUID:

```sql
SELECT row_number, data
FROM stream_writer.rows
WHERE upload_id = '00000000-0000-0000-0000-000000000001'
ORDER BY row_number;
```

Для транзакции записи приложение устанавливает `synchronous_commit=on`.
Приложение не заменяет гарантии хранения PostgreSQL: согласуйте WAL, `fsync`,
`synchronous_commit`, репликацию, резервное копирование и дисковый запас с владельцами БД.
Запись 1–5 ГиБ проходит в одной долгой транзакции. Учитывайте время удержания соединения,
WAL, лаг реплик и место под JSONB/индексы. После прерванного большого COPY невидимые строки
могут занимать место до VACUUM; это не частично опубликованный результат.

## Настройки

Все значения задаются переменными окружения. `.env.example` — образец для локального
стенда; приложение запускайте с явно переданным окружением. Docker Compose сам читает `.env`.

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `WRITER_DATABASE_URL` | обязательна | PostgreSQL DSN |
| `WRITER_API_TOKEN` | обязательна | Bearer token, не менее 32 ASCII-символов без пробелов |
| `WRITER_POOL_SIZE` | `4` | Размер пула; ограничивает параллельные загрузки |
| `WRITER_MAX_UPLOAD_BYTES` | `5368709120` | Максимум исходного потока, 5 ГиБ |
| `WRITER_MAX_LINE_BYTES` | `1048576` | Максимальный размер одной строки, 1 МиБ |
| `WRITER_UPLOAD_TIMEOUT_SECONDS` | `3600` | Общий срок загрузки |
| `WRITER_READ_TIMEOUT_SECONDS` | `30` | Ожидание следующей порции входа |
| `WRITER_ACQUIRE_TIMEOUT_SECONDS` | `5` | Ожидание свободного соединения |
| `WRITER_DB_COMMAND_TIMEOUT_SECONDS` | `60` | Срок обычной команды БД |

Сервис читает поток по мере готовности COPY и не сохраняет полный запрос в памяти.
Лимит строки ограничивает отдельный JSON object, но не гарантирует фиксированный RSS:
одновременно существуют буферы HTTP, парсера, драйвера и структуры Python/JSONB.
При заполненном пуле новый запрос ограничен временем ожидания, а не бесконечной очередью.

`GET /health/live` публичный и проверяет процесс. `GET /health/ready` требует Bearer token
и проверяет доступность БД. Readiness не выполняет init и не доказывает права на COPY.

## Контейнер

Локальный стенд включает PostgreSQL 17 с именованным томом, отдельный `init-db` и сервис
на `127.0.0.1:8080`. Порт PostgreSQL наружу не публикуется.

```bash
cp .env.example .env
docker compose up --build -d writer
docker compose logs writer
```

Демонстрационные пароль и token из `.env.example` предназначены только для локального
стенда. Для других окружений замените их и передавайте секреты штатным способом.
Том PostgreSQL сохраняется после обычного `docker compose down`.

Контейнер работает от UID 10001. В образе установлено приложение из wheel с зависимостями
по lock-файлу; PyInstaller не используется.

Для внешнего доступа нужен TLS reverse proxy: отключите буферизацию тела запроса,
разрешите требуемый объём, согласуйте upload/read timeouts и ограничения соединений
с настройками сервиса. Bearer token передавайте только по HTTPS. Пример Compose использует
обычный HTTP на loopback и незашифрованное соединение БД внутри локального стенда.
Для удалённого PostgreSQL задайте проверку сертификата и имени, например DSN с
`sslmode=verify-full&sslrootcert=/run/secrets/postgres-ca.pem`; путь должен быть доступен
процессу приложения. Пароль с особыми символами в DSN требует URL encoding.

## Проверки

```bash
make check
WRITER_TEST_DATABASE_URL='postgresql://writer:password@localhost:5432/test_writer' make integration
```

Интеграционные тесты запускайте только в выделенной тестовой базе: они создают схему
и тестовые данные. Наличие PostgreSQL обязательно; unit-тесты не подтверждают реальные
COPY/COMMIT/ROLLBACK. Нагрузочные замеры 1–5 ГиБ и отказоустойчивость целевой БД требуют
отдельной приёмки на инфраструктуре эксплуатации.
В PR GitHub Actions проверяет тесты с PostgreSQL 17, wheel и сборку Docker.
Локальные результаты и ограничения приведены в [VALIDATION.md](VALIDATION.md).

## Документация

- [asyncpg: COPY и async iterable](https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.copy_records_to_table)
  — передача записей без списка всего результата.
- [PostgreSQL 17: COPY](https://www.postgresql.org/docs/17/sql-copy.html) — поток по STDIN,
  обработка ошибок и место после отменённой загрузки.
- [PostgreSQL 17: JSON types](https://www.postgresql.org/docs/17/datatype-json.html) —
  ограничения JSONB и отличия от исходного JSON-текста.
- [PostgreSQL 17: WAL reliability](https://www.postgresql.org/docs/17/wal-reliability.html)
  и [VACUUM](https://www.postgresql.org/docs/17/sql-vacuum.html) — серверные гарантии и обслуживание.

План и требования: `openspec/changes/async-postgres-stream-writer/`.
