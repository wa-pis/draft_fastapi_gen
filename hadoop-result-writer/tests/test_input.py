import pandas as pd
import pyarrow as pa
import pytest

from hadoop_result_writer import as_batches


def test_dataframe_infers_from_whole_input_and_drops_index():
    df = pd.DataFrame({"value": [None, None, "hello"]}, index=[9, 8, 7], dtype=object)
    batches = list(as_batches(df, batch_size=2))
    assert [b.num_rows for b in batches] == [2, 1]
    assert batches[0].schema == pa.schema([("value", pa.string())])
    assert batches[1].to_pylist() == [{"value": "hello"}]


def test_dataframe_explicit_types_and_safe_cast():
    schema = pa.schema([("value", pa.int32())])
    assert list(as_batches(pd.DataFrame({"value": [None, 2]}), schema=schema))[0].schema == schema
    with pytest.raises(pa.ArrowInvalid):
        list(as_batches(pd.DataFrame({"value": [2**40]}), schema=schema))
    with pytest.raises(ValueError, match="columns"):
        list(as_batches(pd.DataFrame({"value": [1], "extra": [2]}), schema=schema))


def test_dataframe_all_null_and_nonnullable():
    df = pd.DataFrame({"value": [None, None]}, dtype=object)
    schema = pa.schema([("value", pa.string())])
    assert list(as_batches(df, schema=schema))[0].column(0).null_count == 2
    with pytest.raises(ValueError):
        list(as_batches(df, schema=pa.schema([pa.field("value", pa.string(), nullable=False)])))


def test_python_records_are_lazy_and_typed():
    consumed = []

    def source():
        for value in range(5):
            consumed.append(value)
            yield {"value": value}

    batches = as_batches(source(), schema=pa.schema([("value", pa.int64())]), batch_size=2)
    assert consumed == []
    assert next(batches).num_rows == 2
    assert consumed == [0, 1]
    assert [b.num_rows for b in batches] == [2, 1]


def test_python_records_safe_cast_and_column_order():
    schema = pa.schema([("x", pa.int64()), ("y", pa.string())])
    batch = next(as_batches([{"y": "text", "x": 1}], schema=schema))
    assert batch.schema == schema
    with pytest.raises(pa.ArrowInvalid):
        list(as_batches([{"x": 1.5, "y": "text"}], schema=schema))


@pytest.mark.parametrize("data", [[{"value": 1}], [{"value": 1, "extra": 2}], [{"value": None}]])
def test_records_reject_inference_extra_fields_and_nonnullable(data):
    schema = (
        None
        if len(data[0]) == 1 and data[0]["value"] == 1
        else pa.schema([pa.field("value", pa.int64(), nullable=False)])
    )
    with pytest.raises(ValueError):
        list(as_batches(data, schema=schema))


@pytest.mark.parametrize("kind", ["table", "batch", "stream"])
def test_arrow_inputs_are_bounded(kind):
    batch = pa.record_batch([[1, 2, 3]], names=["value"])
    data = {"table": pa.Table.from_batches([batch]), "batch": batch, "stream": iter([batch])}[kind]
    assert [b.num_rows for b in as_batches(data, batch_size=2)] == [2, 1]


@pytest.mark.parametrize("size", [0, -1, True, 1.5])
def test_invalid_batch_size(size):
    with pytest.raises(ValueError):
        list(as_batches([], batch_size=size))
