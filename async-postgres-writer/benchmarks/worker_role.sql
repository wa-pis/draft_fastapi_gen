-- Run only on the isolated benchmark database, after both schemas are initialized.
SELECT format('CREATE ROLE benchmark_writer LOGIN CONNECTION LIMIT 4 PASSWORD %L', :'password')
\gexec
GRANT USAGE ON SCHEMA stream_writer, stream_writer_bench TO benchmark_writer;
GRANT SELECT, INSERT, UPDATE ON stream_writer.uploads TO benchmark_writer;
GRANT SELECT, INSERT ON stream_writer.rows, stream_writer_bench.typed_rows TO benchmark_writer;
