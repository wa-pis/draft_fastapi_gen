# Потоковая проверка 1 и 5 ГиБ

`stream_load.py` проверяет настоящий путь HTTP → NDJSON parser → asyncpg COPY → COMMIT.
Клиент, HTTP-сервис и PostgreSQL работают отдельными процессами на одной машине.
Скрипт требует dev-зависимости приложения, команду `ps` и пустую выделенную базу;
он не удаляет существующие таблицы или результаты.

## Метод

- Размер входа — ровно 1 073 741 824 или 5 368 709 120 байт, включая JSON и LF.
  Это ГиБ, а не десятичные ГБ.
- Каждая строка занимает 4 096 байт и содержит `n` и `payload`. Значения payload
  генерируются заново для каждой строки из детерминированного PRNG и base64-алфавита.
  Поток не создаётся целиком в памяти или на диске; HTTP-порции — 64 КиБ.
- Таймер записи включает генерацию, SHA-256, передачу HTTP, COPY и подтверждение commit.
  Чтение и повтор измеряются отдельно. Скорость отражает весь локальный путь.
- После commit проверяются receipt и GET. Затем cursor с prefetch 256 читает каждую
  строку: проверяются порядок, поля, номер и SHA-256 восстановленного исходного NDJSON.
  Это возможно для заданного генератором формата; JSONB в общем случае не сохраняет
  исходные пробелы и порядок ключей.
- `--replay` отправляет весь поток ещё раз с тем же UUID. Проверяются `reused=true`,
  прежние receipt/timestamp и неизменное число строк в PostgreSQL.
- RSS клиента, API и максимального отдельного API-backend PostgreSQL снимается каждые
  0,5 с во время записи, чтения и повтора. Короткие пики могут быть пропущены. RSS
  PostgreSQL включает общие страницы и не равен памяти всего кластера. Если не
  передать `--server-pid`, снимается только RSS клиента. Это режим для Docker на Mac:
  PID процессов Linux VM нельзя подставлять в `ps` на хосте.
- Размер таблицы включает TOAST и индексы. WAL измеряется разностью LSN до/после
  фазы записи, включая GET receipt и ожидание диагностики; это объём сгенерированного
  WAL всего кластера, а не прирост каталога `pg_wal` или точный объём одной транзакции.
- `--min-disk-free-gib` задаёт резерв свободного места в ГиБ (по умолчанию 20,
  минимум 1). Если свободное место на томе `--disk-path` падает ниже резерва,
  скрипт прерывает прогон. Если PostgreSQL на другом томе или в VM, следите за
  свободным местом на его томе отдельно.

## Диагностика

`--diagnostics` добавляет наблюдения за фазой записи, а `--label` сохраняет имя
профиля в JSON. Они не меняют проверки receipt, полного чтения и повтора.

- Основной таймер использует `time.monotonic()`. `wall_clock_upload_seconds`
  измеряет ту же фазу записи через `time.time()`, а `wall_minus_monotonic_seconds` показывает
  расхождение. При заметном расхождении исследуйте сон/приостановку системы и изменение
  часов; не используйте такой запуск для сравнения скорости. Во время теста держите
  стенд включённым; на macOS можно запускать команду через `caffeinate -i`.
- Для одного потока каждые 256 МиБ записываются прошедшее время и суммарное время,
  когда генератор приостановлен на `yield`, ожидая HTTP-потребителя. Это прогресс передачи
  на стороне клиента, не число записанных или закоммиченных байт в PostgreSQL.
- Для одного потока `final_tail_seconds` — время от возврата последнего `yield` до HTTP-ответа.
  Оно включает оставшуюся передачу/обработку и ответ сервиса; это не отдельный замер COMMIT.
- Снимки `pg_stat_bgwriter`, `pg_stat_wal`, `pg_stat_database` и, начиная с PostgreSQL 17,
  `pg_stat_checkpointer` дают числовые дельты. Счётчики WAL/checkpointer/bgwriter относятся
  ко всему кластеру, database — ко всей выбранной базе, включая соединения наблюдения.
  Окно начинается до подключения activity-monitor и заканчивается после его остановки
  и дополнительной секунды ожидания публикации статистики. Оно шире HTTP-таймера;
  статистика может запаздывать. `numbackends` сохраняется как снимок, не как дельта.
- Отдельное соединение раз в 0,5 с собирает состояние и wait events API-backend с
  `application_name=stream_writer_benchmark`. Гистограмма считает наблюдения backend,
  не секунды ожидания. `attempts`, `samples`, `timeouts`, `errors`, `shutdown_timeout`,
  `close_error` и `coverage` показывают полноту: пропущенный или неудачный опрос
  не означает отсутствие ожиданий. Ошибка этого monitor не отменяет успешную загрузку;
  ошибки основного соединения и проверки целостности остаются фатальными.

В сохранённых A/B/C/D/E/F использовались разные версии диагностической обвязки: A —
со строгой обработкой ошибок monitor, B/C — с восстановлением после timeout,
D/E — с уточнённым описанием окна и дополнительным `wal_sync_method`, F — также
с сопоставлением обычного и монотонного таймера. Основной таймер
и генерируемая нагрузка одинаковы; SHA-256 скрипта и runtime-кода фиксируются для
каждого запуска. Старое описание scope внутри части JSON не сужает фактическое окно,
описанное выше.

## Повторить

Для каждого объёма создайте отдельную пустую базу с правами на `init-db`.
Пример ниже использует локальный PostgreSQL без TLS. Для другого окружения настройте
проверку сертификата и подходящие пути; не запускайте замер на рабочей базе.

```bash
cd async-postgres-writer
uv sync --locked
export WRITER_DATABASE_URL='postgresql://writer:password@127.0.0.1:5432/stream_load_1g?application_name=stream_writer_benchmark'
export WRITER_TEST_DATABASE_URL="$WRITER_DATABASE_URL"
export WRITER_API_TOKEN=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
.venv/bin/async-postgres-writer init-db
.venv/bin/async-postgres-writer serve --host 127.0.0.1 --port 8080 &
server_pid=$!
# Дождаться готовности API; PID должен относиться к процессу Python, а не к оболочке uv.
curl --fail http://127.0.0.1:8080/health/ready -H "Authorization: Bearer $WRITER_API_TOKEN"
.venv/bin/python benchmarks/stream_load.py \
  --size-gib 1 --server-pid "$server_pid" --replay \
  --disk-path /tmp --output /tmp/stream-load-1gib.json
kill "$server_pid"
wait "$server_pid"
```

Для 5 ГиБ используйте новую пустую базу, новый процесс сервиса, `--size-gib 5` и другой
выходной файл. Лимит приложения по умолчанию допускает ровно 5 ГиБ. Скрипт сохраняет
данные после успеха, чтобы можно было отдельно перезапустить PostgreSQL/API, выполнить
GET по UUID из отчёта и проверить число строк. Удаление выделенной базы выполняется
отдельно после проверки.

## Через Docker Compose с PostgreSQL 17

Запустите Docker и [Compose 2.24.4+](https://docs.docker.com/reference/compose-file/merge/#replace-value)
с поддержкой `!override`, подготовьте dev-окружение (`uv sync --locked`).
Для каждого объёма и уровня параллельности используйте отдельный новый проект и том.
`compose.bulk.yaml` задаёт профиль WAL 8 ГиБ / 16 МиБ. Дополнение `benchmarks/compose.override.yaml`
публикует случайные локальные порты и задаёт `application_name` для диагностики.
Пример для выделенного проекта и Bash:

```bash
cd async-postgres-writer
uv sync --locked
export POSTGRES_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
export WRITER_API_TOKEN=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
compose=(docker compose -p writer-pg17-1g -f compose.yaml -f compose.bulk.yaml -f benchmarks/compose.override.yaml)
"${compose[@]}" up -d --build --wait writer
"${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U writer -d stream_writer \
  -c 'ALTER SYSTEM SET track_io_timing = on' \
  -c 'ALTER SYSTEM SET track_wal_io_timing = on' \
  -c 'SELECT pg_reload_conf()'
sleep 1
"${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U writer -d stream_writer \
  -c 'SHOW track_io_timing' -c 'SHOW track_wal_io_timing'
# Оба значения должны быть on.
"${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U writer -d stream_writer -c 'CHECKPOINT'
sleep 1
"${compose[@]}" exec -T postgres sh -c 'df -h "$PGDATA"'
api_port=$("${compose[@]}" port writer 8080 | cut -d: -f2)
pg_port=$("${compose[@]}" port postgres 5432 | cut -d: -f2)
export WRITER_TEST_DATABASE_URL="postgresql://writer:${POSTGRES_PASSWORD}@127.0.0.1:${pg_port}/stream_writer"
.venv/bin/python benchmarks/stream_load.py --url "http://127.0.0.1:${api_port}" \
  --size-gib 1 --diagnostics --replay --disk-path /tmp \
  --label compose-pg17-bulk-1gib --output /tmp/compose-pg17-1gib.json
```

Для 5 ГиБ замените имя проекта, `--size-gib`, метку и имя отчёта. На Mac можно
запускать клиент через `caffeinate -i`. `--disk-path /tmp` контролирует только диск
хоста. Отдельно отслеживайте `df` на `$PGDATA` внутри Docker на протяжении прогона:
у обоих томов должен оставаться запас 20 ГиБ. При старте 5 ГиБ дополнительно нужно
место под саму запись, индексы и WAL.

Для проверки перезапуска остановите `writer`, штатно перезапустите `postgres` с тем же
томом, дождитесь готовности БД и запустите `writer`. Снова выполните `compose port` для
обоих контейнеров: их случайные порты меняются. GET по UUID из JSON должен вернуть
исходный receipt, а `SELECT count(*) FROM stream_writer.rows WHERE upload_id = '<UUID>'`
— то же число строк. После проверки можно выполнить `"${compose[@]}" down --volumes`
только для созданного тестового проекта.

### Два и четыре одновременных потока по 5 ГиБ

Для каждого сценария повторите настройку Compose выше с новым именем проекта,
например `writer-pg17-2x5g` и `writer-pg17-4x5g`. Именованный том PostgreSQL должен
быть новым: скрипт требует пустую выделенную базу. Оставьте тот же образ PostgreSQL
17 и приложения, профиль WAL 8 ГиБ / 16 МиБ, `shared_buffers=128MB`, оба I/O timing
и `WRITER_POOL_SIZE=4`. `fsync`, `full_page_writes` и `synchronous_commit` остаются
включёнными. Перед каждым замером выполните `CHECKPOINT`. Для сравнения с одним
потоком используйте те же CPU и память VM; при необходимости увеличьте только диск.

После запуска проекта и определения его портов замените вызов клиента на следующий.
Для второго сценария повторите всё с новым томом и `streams=4`:

```bash
streams=2
.venv/bin/python benchmarks/stream_load.py --url "http://127.0.0.1:${api_port}" \
  --size-gib 5 --concurrency "$streams" --diagnostics --replay \
  --disk-path /tmp --min-disk-free-gib 20 \
  --label "compose-pg17-bulk-${streams}x5gib" \
  --output "/tmp/compose-pg17-${streams}x5gib.json"
```

`--size-gib` задаёт размер **каждого** потока. Один клиентский процесс одновременно
отправляет 2 или 4 PUT с разными UUID и одинаковым воспроизводимым NDJSON. Каждый PUT
фиксируется своей транзакцией. Если один PUT завершится ошибкой, уже зафиксированные
загрузки сохранятся; повторите замер с новым томом. После успешной группы скрипт
проверяет receipt, читает все строки и повторяет PUT для каждого UUID последовательно,
вне таймера записи. После штатного перезапуска проверьте receipt и число строк для
всех UUID из `uploads[*].receipt` по новым портам.

- На верхнем уровне JSON `upload_seconds` — время всей группы до последнего
  успешного ответа, а `upload_mib_per_second` — общий входной объём, делённый на это
  время. Это пропускная способность группы; не складывайте скорости отдельных PUT.
- В `uploads` указаны время, скорость, receipt, полное чтение и повтор каждого PUT.
  `total_input_bytes` и `verified_total_rows` суммируют объём и проверенные строки.
- `diagnostics.activity.peak_active_copy_backends` — максимум одновременных `COPY`,
  замеченных в PostgreSQL; `active_copy_backend_histogram` показывает число замеров
  при каждом уровне параллельности.
- `client_upload_cpu_seconds` показывает процессорное время клиента на фазе записи.
  Генерация и SHA-256 всех потоков делят один клиентский процесс, поэтому клиент тоже
  может ограничивать скорость. WAL и диагностические счётчики общие для группы.

Сырые 10 или 20 ГиБ входа не равны расходу диска. Для планирования умножьте расход
таблицы с TOAST и индексами из одиночного замера на число потоков, добавьте запас
для WAL, образов и свободного места. `max_wal_size` — мягкий предел; сгенерированный
WAL и фактический размер `pg_wal` различаются. `--min-disk-free-gib` контролирует
только том хоста, указанный в `--disk-path`. Во время замера отдельно следите за
`df` на `$PGDATA` внутри PostgreSQL, сохраняя резерв и там. Номинальный размер
диска VM не добавляет физического свободного места на хосте.

### A/B: буфер WAL 16 и 64 МиБ

Меняйте только `wal_buffers`. Оставьте `max_wal_size=8GB`, `shared_buffers=128MB`,
прежний `wal_sync_method`, оба I/O timing, пул из четырёх соединений и тот же уровень
параллельности. Используйте одинаковые образы, VM, диски, клиент и входные данные.
На каждый прогон создавайте новый Compose-проект с пустым томом и выполняйте `CHECKPOINT`
перед замером. При наличии места повторите прогоны в порядке 16 → 64 → 64 → 16.

В процедуре Compose выше замените объявление `compose` следующим блоком. Для второго
профиля установите `wal_buffers=64MB`; для каждого повтора задайте новый `run`.

```bash
wal_buffers=16MB
run=1
export POSTGRES_WAL_BUFFERS="$wal_buffers" WRITER_POOL_SIZE=4
project="writer-pg17-wal${wal_buffers%MB}-${run}"
compose=(docker compose -p "$project" -f compose.yaml -f compose.bulk.yaml
         -f benchmarks/compose.override.yaml)
```

Запустите проект, настройте I/O timing и определите порты по инструкции выше.
Проверьте через `SHOW`, что применился нужный `wal_buffers`, а `max_wal_size` и три
настройки надёжной записи совпадают. После `CHECKPOINT` выполните:

```bash
streams=2  # Можно выбрать 4; значение должно совпадать для A и B.
.venv/bin/python benchmarks/stream_load.py --url "http://127.0.0.1:${api_port}" \
  --size-gib 5 --concurrency "$streams" --diagnostics --verify-counts-only \
  --disk-path /tmp --min-disk-free-gib 20 --label "$project" \
  --output "/tmp/${project}.json"
```

Сравнивайте время группы, задержки отдельных PUT и счётчики WAL на ГиБ входа.
`--verify-counts-only` сохраняет прежний таймер записи и проверяет receipt, GET,
число строк по каждому UUID и общий итог. Он не читает содержимое строк из БД,
не вычисляет SHA-256 сохранённых данных и не проверяет повтор PUT. В JSON такой
отчёт помечен `verification_mode: counts_only`.

Для выбранного профиля повторите весь прогон на новом томе с тем же уровнем
параллельности, заменив `--verify-counts-only` на `--replay`. Так скрипт прочитает
все строки, сравнит SHA-256 сохранённого потока и проверит повтор без дубликатов.
После штатного перезапуска проверьте receipt и число строк для всех UUID, как выше.

### A/B: обычная таблица и четыре хеш-партиции

Повторите настройку Compose с `POSTGRES_WAL_BUFFERS=64MB`, `max_wal_size=8GB` и
`WRITER_POOL_SIZE=4`. Оставьте прежние образы, ресурсы VM и параметры надёжной записи.
На каждый прогон нужен новый проект с пустым томом. A использует схему из `init-db`.
Для B после успешного `init-db`, но **до `CHECKPOINT`**, выполните:

```bash
"${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U writer -d stream_writer \
  -f - < benchmarks/partition_rows.sql
```

Скрипт предназначен только для пустой тестовой БД. Он разбивает `stream_writer.rows`
по `HASH (upload_id)` с `MODULUS 4`, сохраняя ограничения таблицы.

Для пары A/B задайте одинаковые UUID в одном порядке: два для 2×5 ГиБ или четыре
для 4×5 ГиБ. В B они должны попасть в разные партиции.
В [отчёте](results/2026-11-10-compose-pg17-toast-partitions.json) выберите сценарий из
`cases` по `label` и возьмите его `uploads[*].receipt.upload_id`.
Заполните Bash-массив `uuids` этими значениями и задайте `project` как имя текущего
тестового проекта. После `CHECKPOINT` и определения портов запустите:

```bash
id_args=()
for uuid in "${uuids[@]}"; do id_args+=(--upload-id "$uuid"); done
.venv/bin/python benchmarks/stream_load.py --url "http://127.0.0.1:${api_port}" \
  --size-gib 5 --concurrency "${#uuids[@]}" "${id_args[@]}" --diagnostics \
  --verify-counts-only --disk-path /tmp --min-disk-free-gib "${reserve_gib:-20}" \
  --label "$project" --output "/tmp/${project}.json"
```

Отдельный контроль 4×5 ГиБ: на новом томе с той же схемой B задайте четыре
разных UUID, которые попадают в `stream_writer.rows_p0`. Не смешивайте его
с B, где загрузки распределены по четырём партициям. После каждого прогона
проверьте размещение для каждого UUID:

```sql
SELECT tableoid::regclass AS leaf
FROM stream_writer.rows
WHERE upload_id = '<UUID>'::uuid
LIMIT 1;
```

Во время записи выборка ниже показывает, какая таблица владеет TOAST, на расширении
которого ждёт `COPY`. Снимайте её с одинаковым интервалом, например 0,5 с.

```sql
SELECT c.oid::regclass AS owning_table, l.relation::regclass AS toast_table,
       count(*) AS waiting_copy_backends
FROM pg_locks l
JOIN pg_stat_progress_copy p USING (pid)
JOIN pg_stat_activity a USING (pid)
JOIN pg_class c ON c.reltoastrelid = l.relation
WHERE l.locktype = 'extend' AND NOT l.granted
  AND a.application_name = 'stream_writer_benchmark'
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
GROUP BY c.oid, l.relation;
```

Отслеживайте свободное место на хосте и на томе `$PGDATA` внутри PostgreSQL.
Для B скрипт суммирует `pg_total_relation_size(relid)` по листьям `pg_partition_tree`:
размер родительской таблицы не включает данные партиций. Сравнивайте одинаковые
объёмы и режимы проверки. Для полного чтения, проверки SHA-256 и повтора PUT
на новом томе замените `--verify-counts-only` на `--replay`. После штатного
перезапуска снова проверьте подтверждения, число строк и их размещение.

### Таблица результатов с 25 типизированными колонками

`stream_writer_bench.typed_rows` — отдельная таблица для тестовых результатов
расчёта. Всего 25 колонок, включая `upload_id` и `row_number` (составной первичный
ключ). Сумма хранится в `numeric(18,2)`, даты и время — в отдельных типах. В таблице
также есть сетевые адреса, массивы, диапазоны, флаги, `bytea` и поле `jsonb` для
дополнительных метрик. HTTP-сервис продолжает писать в `stream_writer.rows`.

Подключитесь к тестовой базе и выполните из каталога приложения:

```bash
psql "$WRITER_TEST_DATABASE_URL" -X -v ON_ERROR_STOP=1 \
  -f benchmarks/typed_rows.sql -f benchmarks/typed_rows_sample.sql
psql "$WRITER_TEST_DATABASE_URL" -X \
  -c 'SELECT upload_id, count(*) FROM stream_writer_bench.typed_rows GROUP BY upload_id;'
```

Пример загружает по пять строк для двух фиксированных UUID, включая значения
`NULL` в необязательных полях. Повторный запуск не добавляет дубликаты. Это
набор для проверки типов и `COPY`, объём нагрузки для замера задаётся отдельно.

#### Сравнение типизированных колонок и JSONB

Оба режима получают один и тот же NDJSON из `typed_data.py`: 4 096 байт на
строку с LF, ровно 1 310 720 строк на 5 ГиБ. UUID передаётся в URL, а номер строки
включён в тело. Случайное заполнение хранится в `description` (или в `metrics.note`
для каждой двадцатой строки). Это данные, которые записывают оба варианта; размер
не набирается пробелами. Остальные поля включают числа, даты, массивы и `NULL`.
JSONB сохраняет весь объект; вариант с колонками берёт уже разобранный и проверенный
объект, преобразует поля в типы PostgreSQL и загружает их двоичным COPY. Повторного
разбора строки в варианте с колонками нет. Время включает преобразование и
подтверждение коммита.

Возьмите массив `compose` из примера выше с новым проектом для каждого сценария.
Установите WAL-буфер 64 МиБ и пул из четырёх соединений. Добавляйте
`compose.typed.yaml` только для варианта с колонками:

```bash
export POSTGRES_WAL_BUFFERS=64MB WRITER_POOL_SIZE=4
layout=columns # или jsonb
if [ "$layout" = columns ]; then
  compose+=(-f benchmarks/compose.typed.yaml)
fi
"${compose[@]}" build writer
"${compose[@]}" up -d --wait postgres
"${compose[@]}" up -d init-db
"${compose[@]}" wait init-db
for sql in typed_rows.sql typed_rows_fk.sql; do
  "${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U writer -d stream_writer \
    -f - < "benchmarks/$sql"
done
"${compose[@]}" up -d --wait writer
```

Два SQL-файла применяются к обоим проектам для одинаковой начальной схемы;
файл с демонстрационными данными не нужен. Обе целевые таблицы журналируются,
имеют составной первичный ключ и внешний ключ на квитанцию. Для каждой загрузки
строки и квитанция коммитятся вместе; `fsync`, `full_page_writes` и
`synchronous_commit` должны быть `on`. Включите I/O timing, выполните
`CHECKPOINT` и получите порты, как в примере выше. Затем:

```bash
streams=2 # или 4, с новым проектом и томом
ids=()
for ((i=1; i<=streams; i++)); do
  ids+=(--upload-id "20000000-0000-4000-8000-00000000000$i")
done
report="/tmp/mixed-$layout-$streams.json"
.venv/bin/python benchmarks/stream_load.py --url "http://127.0.0.1:${api_port}" \
  --mixed-layout "$layout" --size-gib 5 --concurrency "$streams" --diagnostics \
  --disk-path /tmp --verify-counts-only "${ids[@]}" --output "$report"
```

После штатного перезапуска PostgreSQL с тем же томом снова определите порты,
обновите `WRITER_TEST_DATABASE_URL` и запустите:

```bash
.venv/bin/python benchmarks/verify_mixed_restart.py --url "http://127.0.0.1:${api_port}" "$report"
```

Этот шаг сверяет квитанции, общее число строк, границы номеров и все поля первых
32 строк каждой загрузки. Без `--verify-counts-only` загрузчик сверяет каждое
сохранённое значение с детерминированным входом, а `--replay` дополнительно проверяет
повтор без дублей. Чтение и повтор выполняются вне таймера записи. Сравнивайте
скорость между режимами с одним и тем же входом: предыдущие замеры JSONB
использовали другой, двухполевый объект.

#### Один порт и несколько процессов Uvicorn

Профиль обозначается как «процессы Uvicorn × размер пула каждого процесса».
Во всех профилях суммарный верхний предел пула равен четырём:

| Профиль | `BENCHMARK_UVICORN_WORKERS` | `WRITER_POOL_SIZE` | Одновременные PUT |
| --- | ---: | ---: | ---: |
| контроль 1×4 | 1 | 4 | 2 |
| контроль 1×4 | 1 | 4 | 4 |
| 2×2 | 2 | 2 | 2 или 4 |
| 4×1 | 4 | 1 | 4 |

Повторите каждый профиль для `jsonb` и `columns` на новом проекте с пустым томом.
Для прямого сравнения 2 и 4 воркеров установите `streams=4` в обоих профилях:
два воркера получат по два PUT, четыре — по одному.
Используйте одни и те же образы и ресурсы VM во всех сценариях. Перед прогоном
проверьте `wal_buffers=64MB`, `max_wal_size=8GB`, `shared_buffers=128MB`,
`fsync=on`, `full_page_writes=on` и `synchronous_commit=on`. Следите за резервом
на диске хоста и внутри `$PGDATA`; освобождение места в VM не обязательно
сразу уменьшает её виртуальный диск на хосте.
Все процессы слушают один порт одного контейнера; их запускает [Uvicorn
`--workers`](https://uvicorn.dev/deployment/#built-in). Параметр клиента
`--uvicorn-workers` находит процессы по ответам health-проверок, закрепляет за
ними отдельные keep-alive соединения и проверяет PID каждого PUT. Если
последовательные пробы не нашли все процессы, клиент пробует несколько соединений
одновременно. Прогрев проходит до таймера.

Следующий Bash-блок воспроизводит один профиль. Задайте первые четыре переменные
по таблице, увеличивая `run` для повтора:

```bash
cd async-postgres-writer
uv sync --locked
layout=columns                 # или jsonb
workers=2
streams=2
run=1
export BENCHMARK_LAYOUT="$layout" BENCHMARK_UVICORN_WORKERS="$workers"
export WRITER_POOL_SIZE=$((4 / workers)) POSTGRES_WAL_BUFFERS=64MB
export POSTGRES_PASSWORD=$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(24))')
export BENCHMARK_WRITER_PASSWORD=$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(24))')
export WRITER_API_TOKEN=$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))')
case_name="${layout}-w${workers}p${WRITER_POOL_SIZE}-c${streams}-r${run}"
project="writer-pg17-uvicorn-${case_name}"
compose=(docker compose -p "$project" -f compose.yaml -f compose.bulk.yaml
         -f benchmarks/compose.override.yaml -f benchmarks/compose.workers.yaml)

"${compose[@]}" build writer  # один раз на всю серию при неизменном коде
"${compose[@]}" up -d --wait postgres
"${compose[@]}" up -d --no-build init-db
"${compose[@]}" wait init-db
for sql in typed_rows.sql typed_rows_fk.sql; do
  "${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 \
    -U writer -d stream_writer -f - < "benchmarks/$sql"
done
"${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 \
  -U writer -d stream_writer -v password="$BENCHMARK_WRITER_PASSWORD" \
  -f - < benchmarks/worker_role.sql
test "$("${compose[@]}" exec -T postgres psql -X -At -v ON_ERROR_STOP=1 \
  -U writer -d stream_writer \
  -c "SELECT concat(rolconnlimit, '/', rolsuper) FROM pg_roles \
      WHERE rolname='benchmark_writer'")" = 4/false

"${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 \
  -U writer -d stream_writer \
  -c 'ALTER SYSTEM SET track_io_timing = on' \
  -c 'ALTER SYSTEM SET track_wal_io_timing = on' \
  -c 'SELECT pg_reload_conf()'
"${compose[@]}" up -d --wait --no-build writer
"${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 \
  -U writer -d stream_writer -c 'CHECKPOINT'
sleep 1

api_port=$("${compose[@]}" port writer 8080 | cut -d: -f2)
pg_port=$("${compose[@]}" port postgres 5432 | cut -d: -f2)
export WRITER_TEST_DATABASE_URL="postgresql://writer:${POSTGRES_PASSWORD}@127.0.0.1:${pg_port}/stream_writer"
ids=()
for ((i=1; i<=streams; i++)); do
  ids+=(--upload-id "20000000-0000-4000-8000-00000000000$i")
done
report="/tmp/${project}.json"
.venv/bin/python benchmarks/stream_load.py --url "http://127.0.0.1:${api_port}" \
  --mixed-layout "$layout" --size-gib 5 --concurrency "$streams" \
  --uvicorn-workers "$workers" --diagnostics --verify-counts-only \
  --disk-path /tmp --min-disk-free-gib 20 --label "$case_name" \
  "${ids[@]}" --output "$report"
```

`benchmark_writer` — отдельная роль приложения без прав суперпользователя с
[`CONNECTION LIMIT 4`](https://www.postgresql.org/docs/17/sql-createrole.html);
пароль выше подходит для строки подключения без экранирования.
Сервис использует эту роль, а `stream_load.py`, диагностика и административные
команды — роль `writer` через `WRITER_TEST_DATABASE_URL`, поэтому их соединения
не занимают четыре места приложения. Не заменяйте эту строку подключения на
`benchmark_writer`.

В отчёте число разных `uploads[*].worker_pid` должно совпасть с `uvicorn_workers`,
`diagnostics.activity.peak_active_copy_backends` — с `streams`, а
`diagnostics.activity.peak_app_backends` не должно превышать 4; также проверьте
`diagnostics.activity.coverage == "complete"`. По `active_copy_backend_histogram`
видно, как часто все загрузки шли одновременно. Затем выполните описанную выше
проверку перезапуска через `verify_mixed_restart.py`. Для полного прогона на новом
томе замените `--verify-counts-only` на `--replay`. После сохранения отчёта и
проверки удалите только этот тестовый проект: `"${compose[@]}" down --volumes`.

## Сравнить профили A/B

Меняйте по одному фактору, сохраняя PostgreSQL/Python, зависимости, генератор,
HTTP-порции и параметры API. Старый parser берётся из `666b066`, быстрый — из коммита
с оптимизацией parser; запишите полный SHA коммита и хеш фактически запущенного файла.
Для нового воспроизведения используйте одну актуальную версию benchmark со всеми
исправлениями диагностики и сохраните её SHA-256.

| Профиль | Parser | max_wal_size | wal_buffers |
| --- | --- | --- | --- |
| A | старый | 1GB | 4MB |
| B | старый | 8GB | 4MB |
| C | старый | 8GB | 16MB |
| D | быстрый | 8GB | 16MB |

Для всех профилей в `postgresql.conf`:

```conf
shared_buffers = '128MB'
track_io_timing = on
track_wal_io_timing = on
fsync = on
synchronous_commit = on
full_page_writes = on
```

`max_wal_size` и `wal_buffers` задавайте там же по таблице. После изменения
`wal_buffers` обязателен рестарт PostgreSQL; проверьте фактические значения через
`SHOW` и в JSON-отчёте. Остальные настройки, включая checkpoint, оставьте одинаковыми.

1. Создайте новую выделенную пустую базу и запустите новый процесс API для каждого
   профиля. Сервис должен работать с выбранным parser и тем же `application_name`.
2. Выполните `init-db`, дождитесь `/health/ready`, затем выполните `CHECKPOINT`
   от имени пользователя с соответствующими правами **до запуска таймера benchmark**.
3. Запустите пример выше с `--size-gib 5 --diagnostics --label A`, отдельным JSON
   и нужным PID; повторите для B/C/D. Во время сравнения исключите другую нагрузку
   на этот PostgreSQL и диск. Перезапуск не делает файловый кеш ОС одинаково холодным.
4. Дождитесь полного readback и сохранения отчёта. Если запрос записи прошёл, но
   benchmark прервался, не используйте неполный запуск как результат профиля.
5. Остановите API; после проверки receipt удалите только выделенную базу этого
   запуска, затем убедитесь в свободном месте перед следующим профилем.

На диске нужны место под данные/TOAST/индексы и WAL, плюс резерв 20 ГиБ;
`max_wal_size` не является жёстким ограничением размера `pg_wal`. Проверяйте тот же
том через `--disk-path`, сохраняйте отчёты вне удаляемой базы и не удаляйте файлы WAL
вручную. Если поднят отдельный временный кластер, удалять его каталог можно после
остановки PostgreSQL и завершения проверок.

Результаты запусков находятся в `results/`; окружение и ограничения — в
[`../VALIDATION.md`](../VALIDATION.md). Это единичные локальные замеры: они не проверяют
TLS, реплики, failover, потерю питания или масштабирование за пределами проверенных
уровней параллельности.
