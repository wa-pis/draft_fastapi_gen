# Async PostgreSQL Client

Асинхронный Python-клиент для HTTP API [PostgreSQL Stream Writer](../async-postgres-writer/README.md).
Поддерживает Python 3.12+. Клиенту нужны адрес сервера и Bearer token; доступа к
PostgreSQL и установки серверного пакета не требуется.

```bash
python -m pip install ./async-postgres-client
```

Пример отправляет существующий файл без пересериализации, проверяет квитанцию
и читает одну страницу. Для удалённого сервера используйте HTTPS.

```python
import asyncio
import os
from contextlib import aclosing
from pathlib import Path
from uuid import UUID

from async_postgres_client import AsyncPostgresClient


async def file_chunks(path: Path):
    with path.open("rb") as source:
        while chunk := await asyncio.to_thread(source.read, 128 * 1024):
            yield chunk


async def main():
    # Сохраните этот UUID вместе с исходным файлом до отправки.
    upload_id = UUID(os.environ["WRITER_UPLOAD_ID"])
    async with AsyncPostgresClient(
        os.environ["WRITER_API_URL"], os.environ["WRITER_API_TOKEN"], timeout=60.0
    ) as client:
        async with aclosing(file_chunks(Path("input.ndjson"))) as chunks:
            await client.upload_ndjson(upload_id, chunks)
        receipt = await client.get_upload_receipt(upload_id)
        print(receipt.row_count, receipt.sha256)

        page = await client.list_upload_rows(upload_id, limit=100, after_row_number=0)
        print(page.rows)
        # Для следующей страницы передайте page.next_after_row_number, если он не None.
        # По-прежнему можно запросить limit=100, offset=0.


asyncio.run(main())
```

Если ответ на PUT потерян или сервер вернул `503 commit_unknown`, вызовите
`get_upload_receipt` с тем же UUID и сравните SHA-256 с исходными байтами. Ответ
`404` означает, что подтверждённой квитанции пока нет: PUT ещё может выполняться.
Если нужно явно повторить запрос, заново откройте неизменённый файл и используйте
тот же UUID. Клиент не повторяет PUT автоматически.

Полную выгрузку читайте внутри `async with`:

```python
async with client.stream_upload_rows(upload_id) as rows:
    async for row in rows:
        await handle(row.row_number, row.data)
# Здесь поток проверен, если цикл дошёл до конца без исключений.
```

Клиент хранит незавершённую строку в пределах `max_record_bytes` (по умолчанию
16 МиБ) и сверяет число строк с `X-Row-Count`. Длинная строка может пересекать
несколько сетевых порций. Дробные значения и значения с экспонентой возвращаются
как `Decimal`, целые — как `int`. SHA-256 квитанции относится к исходному PUT:
выгрузка JSONB может иметь другой порядок ключей и формат чисел.

Например, чтобы опубликовать CSV только после проверки всего потока:

```python
import csv
from pathlib import Path
from tempfile import NamedTemporaryFile

target = Path("amounts.csv")
with NamedTemporaryFile(
    mode="w", encoding="utf-8", newline="", dir=target.parent, delete=False
) as tmp:
    staging = Path(tmp.name)
    try:
        output = csv.writer(tmp)
        output.writerow(("row_number", "amount"))
        async with client.stream_upload_rows(upload_id) as rows:
            async for row in rows:
                output.writerow((row.row_number, row.data.get("amount")))
        tmp.close()
        staging.replace(target)
    finally:
        staging.unlink(missing_ok=True)
```

`HTTPError` сохраняет статус и доступные детали ошибки сервера.
`TransportError` означает ошибку передачи, `ProtocolError` — нарушение формата
или неполноту ответа без транспортной ошибки. Досрочный выход из контекста
закрывает ответ, но не подтверждает полноту выгрузки.

Контракт: [OpenAPI 3.1 v1](../async-postgres-writer/openapi/stream-writer.v1.yaml).
Тесты: `uv run --locked pytest -q`; для интеграции установите
`WRITER_TEST_API_URL` и `WRITER_API_TOKEN` для отдельного тестового сервера
с PostgreSQL 17 и выполните `uv run --locked pytest -q -m integration`.
