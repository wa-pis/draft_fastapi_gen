# Пакетная и потоковая работа с PostgreSQL из Python

Примеры используют одну тестовую таблицу и один формат: UTF-8 NDJSON со строками
`{"id":10,"name":"орбита","value":12}`. Драйверы подключаются к PostgreSQL напрямую.
Отдельный HTTP-клиент ниже отправляет тот же формат через API сервиса.

| Пример | Запись пакетами | Запись потоком | Чтение потоком |
| --- | --- | --- | --- |
| [psycopg 3](psycopg_example.py) (sync и `--async`) | `executemany` | `COPY FROM STDIN`, `write_row` | Именованный серверный курсор |
| [asyncpg](asyncpg_example.py) | `executemany` | Двоичный `COPY`, `copy_records_to_table` | `cursor` с `prefetch` |
| [SQLAlchemy 2 Async](sqlalchemy_example.py) с asyncpg | Core `execute(insert(...), batch)` | `COPY` через asyncpg в транзакции SQLAlchemy | `AsyncConnection.stream` |

`batch-read` во всех трёх примерах делает `ORDER BY id LIMIT … OFFSET …` с
параметрами. Уникальный ключ задаёт порядок, а `REPEATABLE READ READ ONLY` даёт
один снимок данных на все страницы. PostgreSQL всё равно обрабатывает строки,
пропускаемые через `OFFSET`. Для полной выгрузки большого результата используйте
`stream-read`; для независимых страниц по индексу — `WHERE id > :last_id ORDER BY id`
с `LIMIT`, где `:last_id` обозначает параметр в синтаксисе SQLAlchemy.

## Запуск

Нужны Python 3.12+ и **отдельная тестовая база**. Зависимости примеров перечислены
в [requirements.txt](requirements.txt), они не устанавливаются в сервис.

```bash
cd async-postgres-writer/examples/postgres_io
example_tmp=$(mktemp -d)
python3 -m venv "$example_tmp/venv"
source "$example_tmp/venv/bin/activate"
python -m pip install -r requirements.txt

export POSTGRES_DSN='postgresql://demo:password@127.0.0.1:5432/io_examples'
psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f schema.sql

python psycopg_example.py batch-write rows.ndjson --batch-size 2
python psycopg_example.py batch-read "$example_tmp/psycopg-pages.ndjson" --batch-size 2
python psycopg_example.py stream-read "$example_tmp/psycopg-async-stream.ndjson" --batch-size 2 --async
python asyncpg_example.py stream-read "$example_tmp/asyncpg-stream.ndjson" --batch-size 2
python sqlalchemy_example.py stream-read "$example_tmp/sqlalchemy-stream.ndjson" --batch-size 2
cmp "$example_tmp/psycopg-pages.ndjson" "$example_tmp/psycopg-async-stream.ndjson"
cmp "$example_tmp/psycopg-pages.ndjson" "$example_tmp/asyncpg-stream.ndjson"
cmp "$example_tmp/psycopg-pages.ndjson" "$example_tmp/sqlalchemy-stream.ndjson"
```

Каждый клиент поддерживает все четыре действия: `batch-write`, `batch-read`,
`stream-write` и `stream-read`. В примерах записи ниже используйте пустую таблицу
или файл с новыми `id` для **каждой** команды: повтор того же ID нарушит первичный ключ.
Для подключения вне локального стенда настройте проверку TLS для своего драйвера:
параметры URI `libpq` и `asyncpg` могут отличаться.

```bash
python psycopg_example.py stream-write rows.ndjson --async
python asyncpg_example.py batch-write rows.ndjson --batch-size 2
python sqlalchemy_example.py stream-write rows.ndjson
```

- Все пакеты одного входного файла коммитятся вместе с `synchronous_commit=on`.
  Ошибка в последнем пакете или в конце `COPY` откатывает всю запись. Прямые примеры
  не создают квитанцию или ключ повтора: при потере ответа на `COMMIT` проверяйте
  результат по бизнес-ключу либо используйте HTTP API с UUID загрузки ниже. Для
  гарантий сохранности PostgreSQL также должен работать с `fsync=on`.
- В памяти остаётся пакет, окно выборки или текущая часть потока, в зависимости от
  режима. `--batch-size` задаёт размер пакета, страницы или окна обработки строк;
  драйвер может держать и собственный буфер. Потоковый `COPY` читает вход лениво;
  этот параметр ему не нужен.
- Чтение пишет временный файл рядом с результатом и заменяет результат только
  после полного чтения. Долгая выгрузка удерживает соединение и снимок БД.
  Локальное чтение и запись файлов здесь синхронные, даже у async-драйверов:
  это примеры для запуска отдельной командой.

## Отправка потока в HTTP API сервиса

Запустите [сервис](../../README.md#запуск-локально). Клиент пересылает исходные байты
частями по 256 КиБ, а ответ `201` означает подтверждённый коммит. Сохраните UUID
вместе с файлом до отправки. Если ответ потерялся, запросите квитанцию; при повторе
отправляйте тот же файл с тем же UUID: подтверждённый повтор вернёт `200` без дублей.
`GET` отдаёт только квитанцию — скачивание строк через HTTP сервис не поддерживает.

```bash
export WRITER_API_URL='http://127.0.0.1:8080'
# В терминале клиента задайте тот же WRITER_API_TOKEN, что и у сервиса.
export WRITER_API_TOKEN='local-demo-token-change-before-deploy-1234567890'
upload_id=$(python -c 'from uuid import uuid4; print(uuid4())')
python http_stream_example.py upload "$upload_id" rows.ndjson
python http_stream_example.py receipt "$upload_id"
```

На PostgreSQL 17.11 проверены оба режима записи и чтения для каждого клиента:
пакетная и потоковая выгрузки совпали, ошибка в конце файла и дубль ключа
откатили запись целиком. HTTP-повтор с тем же UUID не добавил строки.

Справка: [PostgreSQL: LIMIT/OFFSET](https://www.postgresql.org/docs/17/queries-limit.html),
[снимки транзакций](https://www.postgresql.org/docs/17/transaction-iso.html),
[psycopg: COPY](https://www.psycopg.org/psycopg3/docs/basic/copy.html) и
[серверные курсоры](https://www.psycopg.org/psycopg3/docs/advanced/cursors.html#server-side-cursors),
[asyncpg: COPY и курсоры](https://magicstack.github.io/asyncpg/current/api/index.html),
[SQLAlchemy: AsyncIO](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#sqlalchemy.ext.asyncio.AsyncConnection.stream),
[доступ к драйверу из SQLAlchemy](https://docs.sqlalchemy.org/en/20/faq/connections.html#accessing-the-underlying-connection-for-an-asyncio-driver),
[HTTPX: потоковые запросы](https://www.python-httpx.org/async/#streaming-requests).
