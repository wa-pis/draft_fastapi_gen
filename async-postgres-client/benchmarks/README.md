# Параллельные выгрузки через async Python-клиент

[Результаты на 9 млн строк](results/2026-11-15-compose-pg17-sdk-read-9m.json) и
[сравнение с прошлым замером](../../async-postgres-writer/VALIDATION.md#чтение-9-млн-строк-через-async-python-клиент).

Поднимите выделенный стенд и загрузите тестовые 9 млн строк по [инструкции для
сервера](../../async-postgres-writer/benchmarks/README.md#параллельные-выгрузки-1-2-и-4-читателя).
Чтобы повторить замер на том же образе PostgreSQL 17.11, укажите для `postgres`
в Compose `image: postgres@sha256:ff87138949ad9a46f32ff23bd6852b52ce024a13dffe9b4087be98292cdd3b4f`.
Оставьте `wal_buffers=64MB`, `fsync=on`, `full_page_writes=on`,
`synchronous_commit=on`, один воркер Uvicorn, `WRITER_POOL_SIZE=4`,
`CONNECTION LIMIT 4` для роли приложения и порции NDJSON до 128 КиБ. Все
запросы читают одну и ту же завершённую загрузку по локальному HTTP без сжатия.

Подготовьте окружение командой `uv sync --locked` и в `async-postgres-writer`,
и в `async-postgres-client`. Из `async-postgres-writer` в той же сессии Bash,
где определён массив Compose `c` и экспортирован `WRITER_API_TOKEN`, выполните:

```bash
set -e
out=$(mktemp -d)
upload_id=90000000-0000-4000-8000-000000000001
for label in sdk-1-start sdk-2 sdk-4 sdk-1-end raw-4; do
  "${c[@]}" up -d --wait --no-build --no-deps --force-recreate writer
  url="http://$("${c[@]}" port writer 8080)"
  if [ "$label" = raw-4 ]; then
    .venv/bin/python benchmarks/read_load.py measure --url "$url" \
      --upload-id "$upload_id" --label "$label" --stream-only --concurrency 4 \
      --compare-stream "$out/sdk-1-start.json" --output "$out/$label.json"
  else
    readers=${label#sdk-}
    (
      cd ../async-postgres-client
      .venv/bin/python benchmarks/read_load.py --url "$url" \
        --upload-id "$upload_id" --label "$label" --concurrency "${readers%%-*}" \
        --expected-bytes 2341158725 \
        --expected-sha256 4b970bbbd31fd195642cf6ac2ed5e12864aa108b57db05b76bcd9f976d9b2f8f \
        --output "$out/$label.json"
    )
  fi
done
printf 'Отчёты: %s\n' "$out"
```

Повтор одиночной выгрузки в конце помогает заметить дрейф стенда. Между
прогонами меняется только контейнер приложения; том PostgreSQL и кэш ОС
сохраняются. Каждый прогон использует один клиентский процесс. SDK разбирает
NDJSON в объекты, проверяет порядок строк и считает SHA-256 по исходным байтам.
Скрипт сервера для контрольного прогона считает строки и SHA-256 без разбора
JSON. Это же делал [клиент предыдущего замера](../../async-postgres-writer/benchmarks/results/2026-11-14-compose-pg17-read-9m-parallel.json).
Оба скрипта обрабатывают тела по мере чтения и пишут на диск только отчёты.

Суммарная скорость — все байты, делённые на время до завершения последней
выгрузки. `ttfb_seconds` измеряет время до первого байта; `first_row_seconds`
у SDK — до первой разобранной строки. Скрипты сохраняют CPU и пиковый RSS
клиента. Для тех же серверных метрик, что в прошлом замере, снимайте до и
после клиентской команды `usage_usec` из `cpu.stat` обоих контейнеров. Память
API берите из `VmHWM` процесса; для PostgreSQL во время каждого прогона
снимайте `memory.current` контейнера (включая кэш страниц) и сумму `RssAnon`
серверных процессов роли приложения, чтобы не складывать общую память.

## Сравнение разбора NDJSON до и после оптимизации

Для [сравнения двух версий](results/2026-11-15-compose-pg17-ndjson-parser-9m.json)
используйте те же 9 млн строк и настройки Compose. Обе версии запускаются
одним скриптом и в одной виртуальной среде; `PYTHONPATH` выбирает исходники
SDK. После серии 1 → 2 → 4 для базовой версии и 4 → 2 → 1 для оптимизированной
повторите пару на четырёх потоках в обратном порядке. Перед каждым прогоном
пересоздавайте контейнер API; том
PostgreSQL сохраняйте, кэш ОС не сбрасывайте. Серверные метрики снимайте по
методике выше вокруг клиентской команды.

Из `async-postgres-writer`, в той же сессии Bash с подготовленным стендом:

```bash
set -euo pipefail
out=$(mktemp -d)
mkdir "$out/baseline"
git -C .. archive 6b57ee50ad33dd94e28b05c5bd69e65bfde2f4e4 async-postgres-client/src |
  tar -x -C "$out/baseline"
for label in before-1 before-2 before-4 after-4 after-2 after-1 after-4-repeat before-4-repeat; do
  sdk_src=../async-postgres-client/src
  if [[ "$label" == before-* ]]; then
    sdk_src="$out/baseline/async-postgres-client/src"
  fi
  "${c[@]}" up -d --wait --no-build --no-deps --force-recreate writer
  url="http://$("${c[@]}" port writer 8080)"
  readers=${label#*-}
  PYTHONPATH="$sdk_src" ../async-postgres-client/.venv/bin/python \
    ../async-postgres-client/benchmarks/read_load.py --url "$url" --label "$label" \
    --upload-id 90000000-0000-4000-8000-000000000001 --concurrency "${readers%%-*}" \
    --expected-bytes 2341158725 \
    --expected-sha256 4b970bbbd31fd195642cf6ac2ed5e12864aa108b57db05b76bcd9f976d9b2f8f \
    --output "$out/$label.json"
done
printf 'Отчёты: %s\n' "$out"
```

## Выборка курсора для двух и четырёх выгрузок

[Результаты эксперимента](results/2026-11-15-compose-pg17-ndjson-prefetch-9m.json).
Используйте тот же выделенный стенд и набор на 9 млн строк. У PostgreSQL
оставьте WAL-буфер 64 МиБ, у приложения — один воркер и четыре подключения.
Пересоздавайте только API между прогонами, оставляя том и кэш PostgreSQL.

Соберите API с поддержкой `WRITER_EXPORT_PREFETCH_ROWS`. Добавьте переменную
`WRITER_EXPORT_PREFETCH_ROWS: ${WRITER_EXPORT_PREFETCH_ROWS:-8}` в `environment`
сервиса `writer` в вашем Compose-файле, если её там ещё нет. Значение 8
воспроизводит прежний размер выборки курсора, 32 — проверяемый вариант.
Лимит порции HTTP остаётся 128 КиБ в обоих случаях. Для проверки влияния
декодера дополнительно прогоните старый SDK из `6b57ee5` при выборке 8
через `git archive` и `PYTHONPATH`, как выше.

Из `async-postgres-writer` с подготовленным стендом и массивом Compose `c`:

```bash
set -e
out=$(mktemp -d)
for label in 8-4 8-2 32-4 32-2 32-2-repeat 8-2-repeat 32-4-repeat 8-4-repeat; do
  export WRITER_EXPORT_PREFETCH_ROWS=${label%%-*}
  readers=${label#*-}
  "${c[@]}" up -d --wait --no-build --no-deps --force-recreate writer
  url="http://$("${c[@]}" port writer 8080)"
  PYTHONPATH=../async-postgres-client/src ../async-postgres-client/.venv/bin/python \
    ../async-postgres-client/benchmarks/read_load_cadence.py --url "$url" --label "$label" \
    --upload-id 90000000-0000-4000-8000-000000000001 --concurrency "${readers%%-*}" \
    --expected-bytes 2341158725 \
    --expected-sha256 4b970bbbd31fd195642cf6ac2ed5e12864aa108b57db05b76bcd9f976d9b2f8f \
    --output "$out/$label.json"
done
printf 'Отчёты: %s\n' "$out"
```

Снимайте `cpu.stat` API и PostgreSQL до и после прогона (`usage_usec`,
`user_usec`, `system_usec`, `nr_throttled`). Для диагностики разницы добавьте
`/proc/net/snmp` PostgreSQL и поля `voluntary_ctxt_switches` и
`nonvoluntary_ctxt_switches` из `/proc/<pid>/status` процессов роли приложения.
Размеры фрагментов в клиентском отчёте описывают чтение HTTP и могут отличаться
от порций, которые отправляет сервер.

## Выборка курсора на широких строках JSONB

[Результаты](results/2026-11-15-compose-pg17-ndjson-prefetch-wide.json).
Подготовьте **пустую выделенную базу** по [инструкции для сервера](../../async-postgres-writer/benchmarks/README.md#параллельные-выгрузки-1-2-и-4-читателя),
но замените команду `read_load.py seed` на следующую. Сохраните
тот же образ PostgreSQL 17.11, WAL-буфер 64 МиБ, один воркер, пул и лимит роли
по четыре подключения и порции NDJSON до 128 КиБ.

```bash
out=$(mktemp -d)
.venv/bin/python ../async-postgres-client/benchmarks/seed_wide.py > "$out/seed.json"
```

Скрипт использует `WRITER_TEST_DATABASE_URL` из настроек стенда. Он создаёт
два набора с псевдослучайным полем Base64 (256 и 768 КиБ), каждый объёмом
около 1,5 ГиБ, и вычисляет эталонный SHA-256 без запроса к HTTP API.
Сделайте `CHECKPOINT` и прогрейте оба набора одной выгрузкой. Прогоны с 8 и 32
строками повторите в обратном порядке для 2 и 4 читателей (порядок указан в
`execution_order` в результатах). Для каждого прогона пересоздавайте оба
контейнера, сохраняя том и кэш ОС. Перед запуском API проверьте `pg_isready`,
перед клиентским замером дождитесь ответа 200 от `/health/ready`.

Для каждой пары числа читателей и размера выборки экспортируйте
`WRITER_EXPORT_PREFETCH_ROWS=8` или `32` перед запуском API. Из каталога
сервера, с `url`, полученным после запуска контейнера, выполните:

```bash
width=768  # или 256
readers=4  # или 2
label="w$width-n$readers-p$WRITER_EXPORT_PREFETCH_ROWS-r1"  # затем r2
fixture=$(jq -ce --arg width "$width" '.profiles[$width]' "$out/seed.json")
PYTHONPATH=../async-postgres-client/src ../async-postgres-client/.venv/bin/python \
  ../async-postgres-client/benchmarks/read_load.py --url "$url" --label "$label" \
  --upload-id "$(jq -er '.upload_id' <<< "$fixture")" --concurrency "$readers" \
  --expected-rows "$(jq -er '.stream.rows' <<< "$fixture")" \
  --expected-bytes "$(jq -er '.stream.bytes' <<< "$fixture")" \
  --expected-sha256 "$(jq -er '.stream.sha256' <<< "$fixture")" \
  --output "$out/$label.json"
```

Снимайте CPU через `cpu.stat` до и после запроса. Для памяти API используйте
`VmHWM` процесса, для PostgreSQL — `memory.peak` всего контейнера, пик
`VmHWM` отдельного процесса роли приложения и максимальную одновременную
сумму `RssAnon` этих процессов по снимкам каждые 0,2 с. `memory.peak` включает
файловый кэш и старт контейнера; сумма `RssAnon` не учитывает общую память и
может пропустить короткий всплеск между снимками.

## Адаптивная выборка курсора

[Результаты](results/2026-11-15-compose-pg17-ndjson-prefetch-adaptive.json).
Для сравнения используйте узкий набор из 9 млн строк и оба широких набора
выше. Скрипты заполнения требуют пустой базы: создайте две базы в одном
PostgreSQL, выполните `init-db` в каждой и выдайте одной читающей роли права
`USAGE` и `SELECT` в обеих. URL приложения должен указывать на базу нужного
набора. Тот же образ API обслуживает все три режима. Оставьте PostgreSQL
17.11, WAL-буфер 64 МиБ, один воркер, пул и лимит роли по четыре соединения
и порции NDJSON до 128 КиБ.

```bash
mode=adaptive  # 8, 32 или adaptive
if [[ "$mode" == adaptive ]]; then
  export WRITER_EXPORT_PREFETCH_ADAPTIVE=true WRITER_EXPORT_PREFETCH_ROWS=8
else
  export WRITER_EXPORT_PREFETCH_ADAPTIVE=false WRITER_EXPORT_PREFETCH_ROWS="$mode"
fi
```

После `CHECKPOINT` прогрейте каждый набор одной выгрузкой. Перед каждым
замером пересоздайте оба контейнера с прежним томом и дождитесь готовности.
Передайте ожидаемые число строк, размер и SHA-256 в `read_load.py`, как выше.
Для 2 и 4 читателей сделайте по два прогона на режим в порядке из
`execution_order`. CPU и пики памяти снимайте тем же способом, что и для
широких строк. Один процесс SDK разбирает NDJSON и считает SHA-256; тело
ответа на диск не сохраняется.

## Смешанные узкие и широкие строки JSONB

[Результаты](results/2026-11-15-compose-pg17-ndjson-prefetch-mixed.json).
Используйте тот же стенд, настройки Compose и три режима выборки, что выше.
В отдельной пустой базе после `init-db` вместо прежнего заполнения запустите
`seed_mixed.py` из каталога сервера. Он сохраняет два набора с одинаковыми
JSONB-полями в разном порядке и выдаёт отдельный эталон SHA-256 для каждого.

```bash
out=$(mktemp -d)
.venv/bin/python ../async-postgres-client/benchmarks/seed_mixed.py > "$out/seed.json"
```

Нужен `WRITER_TEST_DATABASE_URL`, как в предыдущих замерах. После
`CHECKPOINT` прогрейте оба набора одной выгрузкой. Перед каждым прогоном
пересоздайте контейнеры с тем же томом и дождитесь готовности. Используйте
параметры из нужного набора, `alternating` или `bursts`:

```bash
profile=alternating
readers=4
label="w$profile-n$readers-p$mode-r1"  # mode: 8, 32 или adaptive
fixture=$(jq -ce --arg profile "$profile" '.profiles[$profile]' "$out/seed.json")
PYTHONPATH=../async-postgres-client/src ../async-postgres-client/.venv/bin/python \
  ../async-postgres-client/benchmarks/read_load.py --url "$url" --label "$label" \
  --upload-id "$(jq -er '.upload_id' <<< "$fixture")" --concurrency "$readers" \
  --expected-rows "$(jq -er '.stream.rows' <<< "$fixture")" \
  --expected-bytes "$(jq -er '.stream.bytes' <<< "$fixture")" \
  --expected-sha256 "$(jq -er '.stream.sha256' <<< "$fixture")" \
  --output "$out/$label.json"
```

Порядок двух повторов для каждого режима и 2/4 читателей есть в
`execution_order`. CPU и память снимайте тем же способом, что выше.

## Смешанные JSONB с лимитом 2 МиБ на выборку

[Результаты](results/2026-11-15-compose-pg17-ndjson-prefetch-hard-limit.json).
Повторите процедуру со смешанными наборами `alternating` и `bursts` с теми
же параметрами Compose. Для всех запусков выключите адаптивную оценку по
прошлой порции и используйте один образ API. Размер HTTP-порции — 128 КиБ.

```bash
mode=hard   # 8, 32 или hard
export WRITER_EXPORT_PREFETCH_ADAPTIVE=false
if [[ "$mode" == hard ]]; then
  export WRITER_EXPORT_PREFETCH_HARD_LIMIT=true WRITER_EXPORT_PREFETCH_ROWS=8
else
  export WRITER_EXPORT_PREFETCH_HARD_LIMIT=false WRITER_EXPORT_PREFETCH_ROWS="$mode"
fi
```

Пересоздайте оба контейнера с тем же томом и замерьте скриптом `read_load.py`
как выше, сохраняя `mode` в имени прогона. Сделайте по два замера на режим
и 2/4 читателей в порядке `execution_order`. Сверку суммы байтов, полученных
от PostgreSQL за каждую выборку, делайте отдельно от замеров CPU и скорости.

## Жёсткий лимит 2 МиБ: оптимизация CPU

[Результаты](results/2026-11-16-compose-pg17-ndjson-prefetch-hard-limit-cpu.json).
Для режимов `8`, `32` и `hard` соберите API из ревизии `9710162`. Для режима
`optimized` соберите API с текущей правкой. Используйте один том PostgreSQL,
две прежние смешанные выборки и те же параметры стенда: PostgreSQL 17.11,
WAL-буфер 64 МиБ, один воркер, пул и роль с лимитом в четыре соединения и
HTTP-порции до 128 КиБ. Оба режима жёсткого лимита используют
`WRITER_EXPORT_PREFETCH_HARD_LIMIT=true` и `WRITER_EXPORT_PREFETCH_ROWS=8`.

Прогрейте оба набора фиксированной выборкой из 8 строк. Затем повторите
по три прогона для каждого режима, профиля и числа читателей (2 и 4)
в порядке `execution_order`. Перед каждой группой пересоздавайте оба контейнера,
сохраняя том и кэш ОС. Выгрузки проверяйте через `read_load.py` с эталонными
числом строк, объёмом и SHA-256. Снимайте CPU и память как в предыдущем
замере. Проверку байтового лимита проводите отдельно от замеров.
