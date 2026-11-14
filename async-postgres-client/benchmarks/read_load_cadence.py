"""Run the SDK benchmark and report HTTP body chunk sizes and read cadence."""

from collections import Counter
from math import log2
from time import perf_counter

import read_load

_measure = read_load.measure
_body_cadence = []


class _CadenceStream(read_load._MeasuredStream):
    def __init__(self, stream):
        super().__init__(stream)
        self.cadence = Counter()
        self.chunks = Counter()

    async def __aiter__(self):
        iterator = aiter(super().__aiter__())
        while True:
            start = perf_counter()
            try:
                chunk = await anext(iterator)
            except StopAsyncIteration:
                return
            received = perf_counter()
            if not chunk:
                continue
            self.chunks[f"2^{int(log2(len(chunk)))}"] += 1
            self.cadence["chunks"] += 1
            self.cadence["await_next_total_s"] += received - start
            if len(chunk) <= 4096:
                self.cadence["chunks_up_to_4_kib"] += 1
            if len(chunk) == 64 * 1024:
                self.cadence["chunks_of_64_kib"] += 1
            yield chunk
            self.cadence["consume_total_s"] += perf_counter() - received

    async def aclose(self):
        if self.cadence:
            _body_cadence.append({**self.cadence, "body_chunk_size_histogram": dict(self.chunks)})
            self.cadence.clear()
        await super().aclose()


async def measure(*args, **kwargs):
    _body_cadence.clear()
    result = await _measure(*args, **kwargs)
    result["http_body_cadence"] = list(_body_cadence)
    assert len(result["http_body_cadence"]) == result["concurrent_export"]["concurrency"]
    return result


if __name__ == "__main__":
    read_load._MeasuredStream = _CadenceStream
    read_load.measure = measure
    read_load.main()
