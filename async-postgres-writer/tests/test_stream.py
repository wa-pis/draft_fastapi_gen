import asyncio
import hashlib
import json

import pytest

from async_postgres_writer.stream import NDJSONStream
from async_postgres_writer.writer import WriterError


async def chunks(*parts: bytes):
    for part in parts:
        yield part


def stream(source, *, line_limit=1024, body_limit=4096, idle_timeout=1):
    return NDJSONStream(
        source,
        max_line_bytes=line_limit,
        max_upload_bytes=body_limit,
        read_timeout_seconds=idle_timeout,
    )


PAYLOAD = '{"text":"Привет","value":null}\r\n{"nested":{"ok":true}}'.encode()


@pytest.mark.parametrize("boundary", range(len(PAYLOAD) + 1))
async def test_ndjson_handles_arbitrary_utf8_and_delimiter_boundaries(boundary):
    source = stream(chunks(PAYLOAD[:boundary], PAYLOAD[boundary:]))
    result = [(number, json.loads(text)) async for number, text in source]
    assert result == [
        (1, {"text": "Привет", "value": None}),
        (2, {"nested": {"ok": True}}),
    ]
    assert source.row_count == 2
    assert source.byte_count == len(PAYLOAD)
    assert source.sha256 == hashlib.sha256(PAYLOAD).hexdigest()


async def test_parser_applies_backpressure_to_chunk_source():
    seen = []

    async def input_stream():
        seen.append(1)
        yield b'{"id":1}\n'
        seen.append(2)
        yield b'{"id":2}\n'

    source = stream(input_stream()).__aiter__()
    assert await anext(source) == (1, '{"id":1}')
    assert seen == [1]
    assert await anext(source) == (2, '{"id":2}')
    assert seen == [1, 2]
    with pytest.raises(StopAsyncIteration):
        await anext(source)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\n",
        b" \t\n",
        b"{}\n\n",
        b"[]\n",
        b"null\n",
        b"true\n",
        b"1\n",
        b'"text"\n',
        b'{"x":}\n',
        b'{"x":NaN}\n',
        b'{"x":Infinity}\n',
        b'{"x":-Infinity}\n',
        b'{"x":1e9999999999999999999999999}\n',
        b'{"x":1,"x":2}\n',
        b'{"x":{"a":1,"a":2}}\n',
        b'{"x":"\xff"}\n',
        b"{}\r{}\n",
    ],
)
async def test_invalid_streams_fail_instead_of_skipping_rows(payload):
    with pytest.raises(WriterError) as error:
        _ = [item async for item in stream(chunks(payload))]
    assert error.value.status_code == 400
    assert error.value.code == "invalid_stream"


async def test_line_limit_is_checked_before_unterminated_line_is_accumulated():
    read = 0

    async def huge_line():
        nonlocal read
        for _ in range(1000):
            read += 1
            yield b"x" * 8

    with pytest.raises(WriterError) as error:
        _ = [item async for item in stream(huge_line(), line_limit=16)]
    assert error.value.status_code == 413
    assert read <= 3


async def test_limits_include_exact_body_bytes_and_exclude_line_delimiter():
    payload = b"{}\r\n"
    source = stream(chunks(payload), line_limit=2, body_limit=len(payload))
    assert [item async for item in source] == [(1, "{}")]
    assert source.byte_count == 4


async def test_request_limit_does_not_depend_on_content_length():
    with pytest.raises(WriterError) as error:
        _ = [item async for item in stream(chunks(b"{}\n", b"{}\n"), body_limit=5)]
    assert error.value.status_code == 413


async def test_idle_source_times_out():
    never = asyncio.Event()

    async def stalled():
        await never.wait()
        yield b"{}\n"

    with pytest.raises(WriterError) as error:
        _ = [item async for item in stream(stalled(), idle_timeout=0.01)]
    assert error.value.status_code == 408


async def test_source_exception_is_not_reported_as_success():
    async def disconnected():
        yield b"{}\n"
        raise ConnectionError("client disconnected")

    with pytest.raises(ConnectionError):
        _ = [item async for item in stream(disconnected())]


@pytest.mark.parametrize(
    "payload",
    [
        rb'{"value":"ASCII\u0000text"}',
        rb'{"ASCII\u0000key":1}',
        rb'{"value":"\ud800"}',
        rb'{"value":"\udfff"}',
        rb'{"\ud800":1}',
        rb'{"\udfff":1}',
        '{"value":"Привет\\u0000"}'.encode(),
        b'{"value":"raw\tcontrol"}',
    ],
)
async def test_jsonb_unsupported_strings_are_rejected_in_keys_and_values(payload):
    with pytest.raises(WriterError) as error:
        _ = [item async for item in stream(chunks(payload))]
    assert error.value.status_code == 400
    assert error.value.code == "invalid_stream"


@pytest.mark.parametrize(
    "payload,expected",
    [
        (b'{"key":"plain ASCII"}', {"key": "plain ASCII"}),
        ('{"ключ":"Привет 😀"}'.encode(), {"ключ": "Привет 😀"}),
        (rb'{"value":"\ud83d\ude00"}', {"value": "😀"}),
        (rb'{"value":"line\n\t\u001f"}', {"value": "line\n\t\x1f"}),
    ],
)
async def test_jsonb_supported_strings_keep_unicode_and_escaped_controls(payload, expected):
    source = stream(chunks(payload))
    assert [(number, json.loads(text)) async for number, text in source] == [(1, expected)]
    assert source.sha256 == hashlib.sha256(payload).hexdigest()
    assert source.byte_count == len(payload)
