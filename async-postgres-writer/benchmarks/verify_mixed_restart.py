"""Check receipts, row bounds and the first 32 rows per upload after PostgreSQL restarts."""

import argparse
import asyncio
import json
import os
import random
from pathlib import Path
from uuid import UUID

import asyncpg
import httpx
from stream_load import verify_mixed_row
from typed_data import typed_row_bytes


async def verify(report, url):
    layout = report["storage_layout"]
    table, columns = {
        "jsonb": ("stream_writer.rows", "row_number, data"),
        "columns": ("stream_writer_bench.typed_rows", "*"),
    }[layout]
    connection = await asyncpg.connect(os.environ["WRITER_TEST_DATABASE_URL"], command_timeout=60)
    try:
        checked = []
        headers = {"Authorization": f"Bearer {os.environ['WRITER_API_TOKEN']}"}
        async with httpx.AsyncClient(base_url=url, headers=headers) as client:
            for upload in report.get("uploads", [report]):
                receipt = upload["receipt"]
                upload_id = UUID(receipt["upload_id"])
                response = await client.get(f"/v1/uploads/{upload_id}")
                assert response.status_code == 200 and response.json() == receipt
                expected_count = receipt["row_count"]
                count, first, last = await connection.fetchrow(
                    f"SELECT count(*), min(row_number), max(row_number) FROM {table} "
                    "WHERE upload_id=$1",
                    upload_id,
                )
                assert (count, first, last) == (expected_count, 1, expected_count)
                sample_size = min(32, expected_count)
                rows = await connection.fetch(
                    f"SELECT {columns} FROM {table} WHERE upload_id=$1 AND row_number <= $2 "
                    "ORDER BY row_number",
                    upload_id,
                    sample_size,
                )
                generator = random.Random(report["seed"])
                assert len(rows) == sample_size
                for number, row in enumerate(rows, start=1):
                    verify_mixed_row(
                        layout, row, upload_id, number, typed_row_bytes(number, generator)
                    )
                checked.append(
                    {"upload_id": str(upload_id), "row_count": count, "sampled_rows": sample_size}
                )
        assert await connection.fetchval(f"SELECT count(*) FROM {table}") == sum(
            item["row_count"] for item in checked
        )
        assert await connection.fetchval("SELECT count(*) FROM stream_writer.uploads") == len(
            checked
        )
        return {"verified_uploads": checked, "all_receipts_unchanged": True}
    finally:
        await connection.close()


def main():
    if not __debug__:
        raise RuntimeError("Run without -O: verification requires assertions")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(verify(json.loads(args.report.read_text()), args.url)), indent=2))


if __name__ == "__main__":
    main()
