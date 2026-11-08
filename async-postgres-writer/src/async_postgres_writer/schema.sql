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
