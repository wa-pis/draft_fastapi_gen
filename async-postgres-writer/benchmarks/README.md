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
  первого PUT, включая GET receipt и ожидание диагностики; это объём сгенерированного
  WAL всего кластера, а не прирост каталога `pg_wal` или точный объём одной транзакции.
- При свободном месте меньше 20 ГиБ следующий этап прекращается. Каталог проверки
  свободного места должен лежать на том же томе, что и PostgreSQL.

## Диагностика

`--diagnostics` добавляет наблюдения за первым PUT, а `--label` сохраняет имя профиля
в JSON. Они не меняют проверки receipt, полного чтения и повтора.

- Основной таймер использует `time.monotonic()`. `wall_clock_upload_seconds`
  измеряет тот же PUT через `time.time()`, а `wall_minus_monotonic_seconds` показывает
  расхождение. При заметном расхождении исследуйте сон/приостановку системы и изменение
  часов; не используйте такой запуск для сравнения скорости. Во время теста держите
  стенд включённым; на macOS можно запускать команду через `caffeinate -i`.
- Каждые 256 МиБ записываются прошедшее время и суммарное время, когда генератор
  клиента приостановлен на `yield`, ожидая HTTP-потребителя. Это прогресс передачи
  на стороне клиента, не число записанных или закоммиченных байт в PostgreSQL.
- `final_tail_seconds` — время от возврата последнего `yield` до HTTP-ответа.
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
Для каждого объёма используйте отдельный новый проект и том. `compose.bulk.yaml`
задаёт профиль WAL 8 ГиБ / 16 МиБ. Дополнение `benchmarks/compose.override.yaml`
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
TLS, реплики, failover, потерю питания или одновременные загрузки.
