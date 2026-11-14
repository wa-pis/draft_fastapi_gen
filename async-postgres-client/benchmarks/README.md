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
