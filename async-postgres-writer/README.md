# Async PostgreSQL Writer

Отдельное async-приложение принимает HTTP-поток NDJSON и записывает строки в PostgreSQL
через `asyncpg` и `COPY`. Весь результат и receipt фиксируются одной транзакцией.
Приложение не связано с Kafka worker или Hadoop writer и имеет собственные зависимости.

[Примеры на psycopg 3, asyncpg и SQLAlchemy](examples/postgres_io/README.md): пакетная
запись, чтение через `LIMIT/OFFSET`, потоковое чтение и запись через `COPY`, HTTP-клиент.

## Контракт

```text
PUT /v1/uploads/{upload_id}              NDJSON → COPY → COMMIT → receipt
GET /v1/uploads/{upload_id}              квитанция подтверждённой загрузки
GET /v1/uploads/{upload_id}/rows         чтение страницы JSON: limit + after_row_number или offset
GET /v1/uploads/{upload_id}/rows/stream  поток всех строк в NDJSON
```

`upload_id` — UUID, выбранный отправителем до начала загрузки. Для всех этих запросов нужен
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

## Чтение данных через HTTP

Оба режима читают строки одной подтверждённой загрузки по возрастанию `row_number`.
Неизвестный UUID, загрузка в процессе или отменённая загрузка дают `404`.
Ответы помечены `Cache-Control: no-store`. Для чтения не нужны прямой доступ к
PostgreSQL или миграция схемы.

Для чтения страниц передавайте `limit` от 1 до 1000 (по умолчанию 100).
`row_count` — общее число строк загрузки. Если JSON-ответ превышает 16 МиБ,
сервис возвращает `413`: уменьшите `limit` или выберите потоковую выгрузку.

Для больших результатов используйте `after_row_number`: начните с `0`, затем
передавайте `next_after_row_number` из предыдущего ответа. Возвращаются строки
с номером строго больше указанного. `next_after_row_number: null` означает, что
следующей страницы нет. База начинает чтение с нужной позиции по существующему
индексу `(upload_id, row_number)`, не просматривая все предыдущие строки.

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $WRITER_API_TOKEN" \
  "http://127.0.0.1:8080/v1/uploads/$upload_id/rows?limit=1&after_row_number=0"
```

```json
{
  "upload_id": "00000000-0000-0000-0000-000000000001",
  "row_count": 2,
  "limit": 1,
  "after_row_number": 0,
  "next_after_row_number": 1,
  "rows": [{"row_number": 1, "data": {"calculation_id": "calc-42", "value": 12.5}}]
}
```

Для совместимости остаётся `offset` (по умолчанию `0`) и прежний формат ответа.
Если `offset` достиг `row_count`, сервис возвращает `200` с пустым массивом `rows`.
`offset` и `after_row_number` принимают целые числа от `0` до `9223372036854775807`.
Недопустимые значения и одновременная передача обоих параметров (даже с `offset=0`)
дают `422`.

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $WRITER_API_TOKEN" \
  "http://127.0.0.1:8080/v1/uploads/$upload_id/rows?limit=1&offset=0"
```

```json
{
  "upload_id": "00000000-0000-0000-0000-000000000001",
  "row_count": 2,
  "limit": 1,
  "offset": 0,
  "rows": [{"row_number": 1, "data": {"calculation_id": "calc-42", "value": 12.5}}]
}
```

Для полной выгрузки используйте `GET /rows/stream`: ответ `application/x-ndjson`
содержит по одному объекту `{"row_number":1,"data":{...}}` на строку. Поток
читает данные серверным курсором, без накопления всей выборки, и отправляет их
порциями до 128 КиБ. Границы порций в сети могут измениться; длинная строка может
занимать несколько порций. При чтении разбирайте поток по переводам строки.
Страницы и поток возвращают сохранённый JSONB, поэтому исходные байты, порядок
ключей и SHA-256 входного файла могут не совпасть с выгрузкой.

```bash
(
  set -eu
  download_dir=$(mktemp -d ./.download.XXXXXX)
  trap 'rm -f "$download_dir/headers" "$download_dir/result.ndjson"; rmdir "$download_dir"' EXIT
  curl --fail --no-buffer --dump-header "$download_dir/headers" \
    --output "$download_dir/result.ndjson" \
    -H "Authorization: Bearer $WRITER_API_TOKEN" \
    "http://127.0.0.1:8080/v1/uploads/$upload_id/rows/stream"
  expected=$(awk 'tolower($1) == "x-row-count:" { gsub(/\r/, "", $2); print $2 }' \
    "$download_dir/headers")
  actual=$(wc -l < "$download_dir/result.ndjson" | tr -d '[:space:]')
  if [ "$actual" = "$expected" ]; then
    mv "$download_dir/result.ndjson" result.ndjson
  else
    echo "Выгрузка неполная: ожидалось $expected, получено $actual" >&2
    exit 1
  fi
)
```

Перед использованием файла убедитесь, что запрос завершился успешно и число строк
совпало с `X-Row-Count` из заголовков. Ошибка БД до начала ответа даёт `503`;
после отправки заголовков статус уже нельзя изменить, и поток прерывается.
Неполный файл удалите и повторите GET. Каждый открытый поток удерживает соединение
из общего пула до завершения, обрыва или таймаута. Для полного скачивания
используйте поток, а для чтения отдельных страниц — `after_row_number`.

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
в `stream_writer.uploads`. При прямом подключении к БД выбирайте строки по
подтверждённому UUID:

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
| `WRITER_POOL_SIZE` | `4` | Размер общего пула для записи и чтения |
| `WRITER_MAX_UPLOAD_BYTES` | `5368709120` | Максимум исходного потока, 5 ГиБ |
| `WRITER_MAX_LINE_BYTES` | `1048576` | Максимальный размер одной строки, 1 МиБ |
| `WRITER_UPLOAD_TIMEOUT_SECONDS` | `3600` | Общий срок загрузки |
| `WRITER_EXPORT_TIMEOUT_SECONDS` | `3600` | Общий срок `GET /rows/stream`, включая передачу ответа клиенту |
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
docker compose build writer
docker compose up --no-build -d writer
docker compose logs writer
```

Демонстрационные пароль и token из `.env.example` предназначены только для локального
стенда. Для других окружений замените их и передавайте секреты штатным способом.
Том PostgreSQL сохраняется после обычного `docker compose down`.

Для загрузок 1–5 ГиБ есть отдельный профиль настроек PostgreSQL:

```bash
docker compose -f compose.yaml -f compose.bulk.yaml build writer
docker compose -f compose.yaml -f compose.bulk.yaml up --no-build -d writer
```

Он задаёт `max_wal_size=8GB` и `wal_buffers=16MB`. На локальном PostgreSQL 14 эти
параметры уменьшили частоту checkpoint и число вынужденных записей WAL; сравнение
и ограничения — в [VALIDATION.md](VALIDATION.md#оптимизация-потока-5-гиб).
Сервис продолжает использовать одну транзакцию и `synchronous_commit=on`.
Для внешней БД эти параметры настраивает её администратор: приложение их не меняет.
`wal_buffers` требует перезапуска PostgreSQL. `max_wal_size` — мягкий предел:
предусмотрите место под WAL, данные, индексы и удержание WAL репликами/архиватором;
интервал между checkpoint может увеличить время восстановления после сбоя.
Основание: [bulk COPY](https://www.postgresql.org/docs/14/populate.html#POPULATE-MAX-WAL-SIZE)
и [настройки WAL](https://www.postgresql.org/docs/14/runtime-config-wal.html).
Профиль проверен с PostgreSQL 17 в локальном Compose. См. [сравнение замеров](VALIDATION.md#docker-compose-и-postgresql-17)
и [инструкцию по повтору](benchmarks/README.md#через-docker-compose-с-postgresql-17).
Для параллельных загрузок можно задать `POSTGRES_WAL_BUFFERS=64MB` перед запуском
с `compose.bulk.yaml`. Без этой переменной действует прежний буфер 16 МиБ. Параметры
надёжной записи не меняются. См. [замеры параллельной записи](VALIDATION.md#повтор-параллельных-замеров-и-настройка-wal-буфера).

Контейнер работает от UID 10001. В образе установлено приложение из wheel с зависимостями
по lock-файлу; PyInstaller не используется.

Для внешнего доступа нужен TLS reverse proxy: отключите буферизацию тела запроса
для загрузки и тела ответа для потоковой выгрузки. Разрешите требуемый объём и
согласуйте таймауты и ограничения соединений
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
COPY/COMMIT/ROLLBACK. Локальные HTTP-проверки на 1 и 5 ГиБ прошли: полное чтение с
SHA-256, повтор без дубликатов и сохранность после штатного перезапуска PostgreSQL.
Скрипт повторения — [benchmarks/README.md](benchmarks/README.md).
Нагрузка и отказоустойчивость целевой БД требуют отдельной приёмки на инфраструктуре эксплуатации.
В PR GitHub Actions проверяет тесты с PostgreSQL 17, wheel и сборку Docker.
Локальные результаты и ограничения приведены в [VALIDATION.md](VALIDATION.md).

## Документация

- [Контракт OpenAPI 3.1 v1](openapi/stream-writer.v1.yaml) — исходник для
  `GET /openapi.json` и Swagger UI на `GET /docs`. Контракт описывает Bearer,
  статусы и модели ответа; примеры PUT, квитанции, страниц и потоковой выгрузки
  приведены выше. Для больших ответов используйте потоковый клиент.
- [Асинхронный Python-клиент](../async-postgres-client/README.md) — отдельный
  пакет для работы с тем же контрактом без подключения к PostgreSQL.
- [asyncpg: COPY и async iterable](https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.copy_records_to_table)
  — передача записей без списка всего результата.
- [PostgreSQL 17: COPY](https://www.postgresql.org/docs/17/sql-copy.html) — поток по STDIN,
  обработка ошибок и место после отменённой загрузки.
- [PostgreSQL 17: JSON types](https://www.postgresql.org/docs/17/datatype-json.html) —
  ограничения JSONB и отличия от исходного JSON-текста.
- [PostgreSQL 17: WAL reliability](https://www.postgresql.org/docs/17/wal-reliability.html)
  и [VACUUM](https://www.postgresql.org/docs/17/sql-vacuum.html) — серверные гарантии и обслуживание.

План и требования:

- [Текущий PostgreSQL-сервис](openspec/changes/async-postgres-stream-writer/proposal.md).
- [Сервер и версионируемый контракт OpenAPI](openspec/changes/postgres-stream-openapi-server/proposal.md).
- [Отдельный асинхронный Python-клиент](../async-postgres-client/openspec/changes/postgres-stream-openapi-client/proposal.md).
