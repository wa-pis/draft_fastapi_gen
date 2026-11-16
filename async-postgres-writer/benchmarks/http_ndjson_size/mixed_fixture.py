"""Deterministic bounded NDJSON fixture for PostgreSQL HTTP ingestion benchmarks."""

import argparse
import hashlib
import json
from base64 import b64encode
from collections.abc import Iterable, Iterator

_DEFAULT_BYTES = 5 * 1024**3
_CHUNK_BYTES = 1024**2
_PREFIX = b'{"payload":"'
_SUFFIX = b'"}\n'
_LINE_OVERHEAD = len(_PREFIX + _SUFFIX)
_WIDTHS = (1024, 768 * 1024, 1024, 256 * 1024, 1024, 256 * 1024, 1024, 256 * 1024)


def _line(width: int, identity: bytes) -> bytes:
    digest = hashlib.shake_256(identity).digest((width * 3 + 3) // 4)
    return _PREFIX + b64encode(digest)[:width] + _SUFFIX


def _pack(lines: Iterable[bytes]) -> Iterator[bytes]:
    batch, size = [], 0
    for line in lines:
        if size + len(line) > _CHUNK_BYTES:
            yield b"".join(batch)
            batch, size = [], 0
        batch.append(line)
        size += len(line)
    if batch:
        yield b"".join(batch)


class MixedFixture:
    """Prebuild four high-entropy width cycles and replay them from shared memory.

    The last row may be resized so the body always has exactly total_bytes.
    A new chunks() iterator is independent; streams reuse immutable chunks.
    """

    def __init__(self, total_bytes: int = _DEFAULT_BYTES, seed: int = 20261116):
        if total_bytes < _LINE_OVERHEAD:
            raise ValueError(f"Expected at least {_LINE_OVERHEAD} bytes for one NDJSON object")
        self.total_bytes = total_bytes
        self._sha256 = None
        self._payload_signature = None
        templates = tuple(
            _line(width, f"{seed}:{index}:{width}".encode("ascii"))
            for index, width in enumerate(_WIDTHS * 4)
        )
        template_bytes = sum(map(len, templates))
        self._copies, remaining = divmod(total_bytes, template_bytes)
        if 0 < remaining < _LINE_OVERHEAD:
            self._copies -= 1
            remaining += template_bytes
        self._cycle_chunks = tuple(_pack(templates)) if self._copies else ()

        tail = []
        for line in templates:
            if not remaining:
                break
            if remaining < len(line) + _LINE_OVERHEAD:
                if remaining <= len(line):
                    tail.append(line[: remaining - len(_SUFFIX)] + _SUFFIX)
                else:
                    extra = b64encode(hashlib.shake_256(f"{seed}:tail".encode("ascii")).digest(12))
                    tail.append(line[: -len(_SUFFIX)] + extra[: remaining - len(line)] + _SUFFIX)
                remaining = 0
            else:
                tail.append(line)
                remaining -= len(line)
        if remaining:
            raise RuntimeError("Incomplete mixed NDJSON tail")
        self._tail_chunks = tuple(_pack(tail))
        self.rows = self._copies * len(templates) + len(tail)

    def chunks(self) -> Iterator[bytes]:
        """Return a fresh iterator over raw NDJSON chunks, each at most 1 MiB."""
        for _ in range(self._copies):
            yield from self._cycle_chunks
        yield from self._tail_chunks

    def expected(self) -> dict[str, str | int]:
        """Return expected input receipt metadata, hashing the body only the first time."""
        if self._sha256 is None:
            digest = hashlib.sha256()
            for chunk in self.chunks():
                digest.update(chunk)
            self._sha256 = digest.hexdigest()
        return {"rows": self.rows, "bytes": self.total_bytes, "sha256": self._sha256}

    def expected_payload_signature(self) -> str:
        """SHA-256 of ordered, concatenated lowercase SHA-256 hex digests of payload strings."""
        if self._payload_signature is None:
            digest = hashlib.sha256()
            chunk_signatures: dict[bytes, bytes] = {}
            row_signatures: dict[bytes, bytes] = {}
            for chunk in self.chunks():
                if (signature := chunk_signatures.get(chunk)) is None:
                    parts = []
                    for line in chunk.splitlines(keepends=True):
                        if (part := row_signatures.get(line)) is None:
                            payload = memoryview(line)[len(_PREFIX) : -len(_SUFFIX)]
                            part = hashlib.sha256(payload).hexdigest().encode("ascii")
                            row_signatures[line] = part
                        parts.append(part)
                    signature = b"".join(parts)
                    chunk_signatures[chunk] = signature
                digest.update(signature)
            self._payload_signature = digest.hexdigest()
        return self._payload_signature

    def expected_export_bytes(self) -> int:
        """Return expected sum of stored sizes, including the PG JSONB envelope and LF."""
        example = b'{"data": {"payload": ""}, "row_number": 1}\n'
        overhead = len(example) - _LINE_OVERHEAD - 1
        return (
            self.total_bytes
            + overhead * self.rows
            + sum(len(str(row_number)) for row_number in range(1, self.rows + 1))
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bytes", type=int, default=_DEFAULT_BYTES, dest="total_bytes")
    parser.add_argument("--seed", type=int, default=20261116)
    fixture = MixedFixture(**vars(parser.parse_args()))
    print(json.dumps({**fixture.expected(), "export_bytes": fixture.expected_export_bytes()}))
