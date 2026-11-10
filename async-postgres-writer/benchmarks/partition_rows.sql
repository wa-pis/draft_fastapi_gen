-- Benchmark-only replacement for a fresh, empty dedicated database.
BEGIN;

LOCK TABLE stream_writer.uploads, stream_writer.rows IN ACCESS EXCLUSIVE MODE;

DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_class
        WHERE oid IN (
            'stream_writer.uploads'::regclass,
            'stream_writer.rows'::regclass
        )
          AND (relkind <> 'r' OR relpersistence <> 'p')
    ) THEN
        RAISE EXCEPTION 'partition fixture requires ordinary logged tables';
    END IF;

    IF EXISTS (SELECT 1 FROM stream_writer.uploads LIMIT 1)
       OR EXISTS (SELECT 1 FROM stream_writer.rows LIMIT 1) THEN
        RAISE EXCEPTION 'partition fixture requires empty uploads and rows tables';
    END IF;
END
$guard$;

ALTER TABLE stream_writer.rows RENAME TO rows_unpartitioned_old;

CREATE TABLE stream_writer.rows (
    LIKE stream_writer.rows_unpartitioned_old INCLUDING ALL,
    CONSTRAINT rows_upload_id_fkey
        FOREIGN KEY (upload_id) REFERENCES stream_writer.uploads(upload_id)
) PARTITION BY HASH (upload_id);

CREATE TABLE stream_writer.rows_p0 PARTITION OF stream_writer.rows
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE stream_writer.rows_p1 PARTITION OF stream_writer.rows
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE stream_writer.rows_p2 PARTITION OF stream_writer.rows
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE stream_writer.rows_p3 PARTITION OF stream_writer.rows
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

DROP TABLE stream_writer.rows_unpartitioned_old;

COMMIT;
