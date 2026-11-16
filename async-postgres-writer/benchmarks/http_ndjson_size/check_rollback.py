"""Outside measurement: a malformed tail must roll back both rows and receipt."""

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import httpx
from mixed_fixture import MixedFixture
from stack import api_url, inspect_project, psql


async def check(output):
    inspect_project("postgres", "writer")
    fixture = MixedFixture(8 * 1024**2)
    valid = b'{"payload":"retry"}\n'
    url = api_url()
    headers = {
        "Authorization": f"Bearer {os.environ['WRITER_API_TOKEN']}",
        "Content-Type": "application/x-ndjson",
    }
    results = {}
    async with httpx.AsyncClient(
        base_url=url, headers=headers, timeout=60, trust_env=False
    ) as client:
        for index, mode in enumerate(("DISABLE", "ENABLE ALWAYS"), 1):
            upload_id = uuid4()
            target = f"/v1/uploads/{upload_id}"
            psql(f"ALTER TABLE stream_writer.rows {mode} TRIGGER rows_set_ndjson_bytes;")

            async def body():
                for chunk in fixture.chunks():
                    yield chunk
                yield b'{"malformed":\n'

            response = await client.put(target, content=body())
            assert response.status_code == 400, response.text
            status = (await client.get(target)).status_code
            assert status == 404, status
            counts = psql(f"""
                SELECT concat(
                    (SELECT count(*) FROM stream_writer.uploads WHERE upload_id='{upload_id}'),
                    '/',
                    (SELECT count(*) FROM stream_writer.rows WHERE upload_id='{upload_id}'));
            """)
            assert counts == "0/0", counts
            retry = await client.put(target, content=valid)
            assert retry.status_code == 201, retry.text
            assert retry.json()["sha256"] == hashlib.sha256(valid).hexdigest()
            assert retry.json()["row_count"] == 1 and retry.json()["reused"] is False
            stored_size = psql(
                f"SELECT ndjson_bytes FROM stream_writer.rows WHERE upload_id='{upload_id}'"
            )
            assert stored_size == ("48" if index == 2 else ""), stored_size
            results[mode] = {
                "malformed_payload_status": response.status_code,
                "receipt_lookup_status": status,
                "committed_receipts_after_failure": 0,
                "committed_rows_after_failure": 0,
                "same_id_retry_status": retry.status_code,
                "retry_rows": 1,
                "retry_stored_size": int(stored_size) if stored_size else None,
            }
    output.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    asyncio.run(check(parser.parse_args().output))
