.PHONY: check integration image

check:
	uv run --locked ruff check .
	uv run --locked ruff format --check .
	uv run --locked pytest -q
	OPENSPEC_TELEMETRY=0 openspec validate --all --strict

integration:
	@test -n "$${WRITER_TEST_DATABASE_URL:-}" || (echo 'Set WRITER_TEST_DATABASE_URL to a dedicated test database'; exit 1)
	uv run --locked pytest -q -m integration

image:
	docker build -t async-postgres-writer:local .
