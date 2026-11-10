-- Logged calculation results for the benchmark; independent of the writer schema.
CREATE SCHEMA IF NOT EXISTS stream_writer_bench;

CREATE TABLE IF NOT EXISTS stream_writer_bench.typed_rows (
    upload_id uuid NOT NULL,
    row_number bigint NOT NULL CHECK (row_number > 0),
    scenario_id integer NOT NULL,
    model_version smallint NOT NULL,
    account_code varchar(32) NOT NULL,
    currency_code char(3) NOT NULL,
    description text,
    is_final boolean NOT NULL,
    amount numeric(18, 2) NOT NULL,
    confidence real NOT NULL,
    score double precision NOT NULL,
    calculation_date date NOT NULL,
    calculated_at timestamp with time zone NOT NULL,
    source_local_at timestamp without time zone,
    cutoff_time time without time zone,
    processing_time interval NOT NULL,
    source_ip inet,
    source_network cidr,
    labels text[] NOT NULL,
    sample_ids bigint[] NOT NULL,
    validity daterange NOT NULL,
    metrics jsonb CHECK (jsonb_typeof(metrics) = 'object'),
    fingerprint bytea,
    confidence_band numrange,
    quality_flags bit(8) NOT NULL,
    PRIMARY KEY (upload_id, row_number)
);
