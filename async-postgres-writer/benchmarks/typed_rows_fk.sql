-- Only for the HTTP benchmark, after init-db and typed_rows.sql in a fresh database.
ALTER TABLE stream_writer_bench.typed_rows
    ADD CONSTRAINT typed_rows_upload_id_fkey
    FOREIGN KEY (upload_id) REFERENCES stream_writer.uploads(upload_id);
