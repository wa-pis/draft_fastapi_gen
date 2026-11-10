-- Ten repeatable example rows; the fifth row of each upload has nullable fields unset.
WITH sample_uploads (upload_id, scenario_id, model_version, account_prefix, currency_code) AS (
    VALUES
        ('00000000-0000-4000-8000-000000000001'::uuid, 101, 1, 'ALPHA', 'USD'),
        ('00000000-0000-4000-8000-000000000002'::uuid, 202, 2, 'BETA', 'EUR')
)
INSERT INTO stream_writer_bench.typed_rows (
    upload_id, row_number, scenario_id, model_version, account_code, currency_code,
    description, is_final, amount, confidence, score, calculation_date, calculated_at,
    source_local_at, cutoff_time, processing_time, source_ip, source_network,
    labels, sample_ids, validity, metrics, fingerprint, confidence_band, quality_flags
)
SELECT
    upload_id,
    n::bigint,
    scenario_id,
    model_version::smallint,
    account_prefix || '-' || lpad(n::text, 2, '0'),
    currency_code::char(3),
    CASE WHEN n < 5 THEN format('Sample calculation %s for %s', n, account_prefix) END,
    n >= 3,
    (scenario_id + n * 12.25)::numeric(18, 2),
    (0.50 + n * 0.05)::real,
    (scenario_id / 10.0 + n * 0.125)::double precision,
    DATE '2026-01-01' + (n - 1),
    TIMESTAMPTZ '2026-01-01 12:00:00+00' + (n - 1) * INTERVAL '24 hours',
    CASE WHEN n < 5 THEN TIMESTAMP '2026-01-01 15:00:00' + (n - 1) * INTERVAL '1 day' END,
    CASE WHEN n < 5 THEN TIME '17:00:00' + n * INTERVAL '5 minutes' END,
    n * INTERVAL '125 milliseconds',
    CASE WHEN n < 5 THEN ('192.0.2.' || n)::inet END,
    CASE WHEN n < 5 THEN CIDR '192.0.2.0/24' END,
    ARRAY['sample', account_prefix],
    ARRAY[scenario_id::bigint * 100 + n, scenario_id::bigint * 100 + n + 10],
    daterange(DATE '2026-01-01' + (n - 1), DATE '2026-02-01' + (n - 1), '[)'),
    CASE WHEN n < 5 THEN jsonb_build_object('items', n, 'origin', 'sample') END,
    CASE WHEN n < 5 THEN sha256(convert_to(upload_id::text || ':' || n::text, 'UTF8')) END,
    CASE WHEN n < 5 THEN numrange(0.45 + n * 0.05, 0.55 + n * 0.05, '[)') END,
    (1 << (n - 1))::bit(8)
FROM sample_uploads
CROSS JOIN generate_series(1, 5) AS numbers(n)
ON CONFLICT (upload_id, row_number) DO NOTHING;
