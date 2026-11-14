import argparse
import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import aclosing
from pathlib import Path
from uuid import UUID

import httpx


async def chunks(path: Path) -> AsyncIterator[bytes]:
    with path.open("rb") as source:
        while chunk := source.read(256 * 1024):
            yield chunk


async def run(args: argparse.Namespace, url: str, token: str) -> None:
    async with httpx.AsyncClient(
        base_url=url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=httpx.Timeout(60, connect=5),
    ) as client:
        endpoint = f"/v1/uploads/{args.upload_id}"
        if args.action == "upload":
            async with aclosing(chunks(args.path)) as body:
                response = await client.put(
                    endpoint,
                    content=body,
                    headers={"Content-Type": "application/x-ndjson"},
                )
        else:
            response = await client.get(endpoint)
        response.raise_for_status()
        print(response.text)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream NDJSON to async-postgres-writer")
    actions = parser.add_subparsers(dest="action", required=True)
    upload = actions.add_parser("upload")
    upload.add_argument("upload_id", type=UUID)
    upload.add_argument("path", type=Path)
    actions.add_parser("receipt").add_argument("upload_id", type=UUID)
    args = parser.parse_args()
    url = os.environ.get("WRITER_API_URL", "http://127.0.0.1:8080")
    token = os.environ.get("WRITER_API_TOKEN")
    if not token:
        parser.error("set WRITER_API_TOKEN")
    asyncio.run(run(args, url, token))


if __name__ == "__main__":
    main()
