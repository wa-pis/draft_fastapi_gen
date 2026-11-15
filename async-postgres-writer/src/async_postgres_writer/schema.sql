CREATE SCHEMA IF NOT EXISTS stream_writer;

CREATE TABLE IF NOT EXISTS stream_writer.uploads (
    upload_id uuid PRIMARY KEY,
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    byte_count bigint NOT NULL CHECK (byte_count >= 0),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS stream_writer.rows (
    upload_id uuid NOT NULL REFERENCES stream_writer.uploads(upload_id),
    row_number bigint NOT NULL CHECK (row_number > 0),
    data jsonb NOT NULL CHECK (jsonb_typeof(data) = 'object'),
    PRIMARY KEY (upload_id, row_number)
);

-- Leave existing rows NULL so this upgrade does not rewrite the table.
ALTER TABLE stream_writer.rows ADD COLUMN IF NOT EXISTS ndjson_bytes integer;

CREATE OR REPLACE FUNCTION stream_writer.set_ndjson_bytes()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    NEW.ndjson_bytes :=
        CASE WHEN getdatabaseencoding() = 'UTF8' THEN octet_length(NEW.data::text)
             ELSE octet_length(convert_to(NEW.data::text, 'UTF8')) END
        + length(NEW.row_number::text)
        + length('{"data": ' || ', "row_number": ' || '}') + 1;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER rows_set_ndjson_bytes
BEFORE INSERT OR UPDATE OF data, row_number, ndjson_bytes ON stream_writer.rows
FOR EACH ROW EXECUTE FUNCTION stream_writer.set_ndjson_bytes();

-- Replication updates can change the JSONB just like application writes.
ALTER TABLE stream_writer.rows ENABLE ALWAYS TRIGGER rows_set_ndjson_bytes;
