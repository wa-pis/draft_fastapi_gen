import sys
from contextlib import closing
from importlib import import_module
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
example = import_module("examples.postgres_io.common")


def test_example_validates_input_as_it_streams(tmp_path):
    source = tmp_path / "input.ndjson"
    source.write_text(
        '{"id": 1, "name": "орбита", "value": -7}\n{"id": 2, "name": "bad", "value": true}\n',
        encoding="utf-8",
    )
    with closing(example.rows(source)) as stream:
        assert next(stream) == (1, "орбита", -7)
        with pytest.raises(ValueError, match=r"input\.ndjson:2:"):
            next(stream)


def test_example_keeps_previous_export_after_failure(tmp_path):
    target = tmp_path / "output.ndjson"
    target.write_text("old", encoding="utf-8")
    with (
        pytest.raises(RuntimeError, match="export interrupted"),
        example.atomic_output(target) as out,
    ):
        example.write_row(out, (1, "new", 0))
        raise RuntimeError("export interrupted")
    assert target.read_text(encoding="utf-8") == "old"
    assert list(tmp_path.iterdir()) == [target]
