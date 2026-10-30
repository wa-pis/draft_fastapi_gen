.PHONY: check demo image integration
check:
	uv run --locked pytest -q
	uv run --locked ruff check .
	uv run --locked ruff format --check .
	openspec validate standalone-hdfs-hive-writer --strict

demo:
	uv run --locked hadoop-result-writer demo --directory /tmp/hrw-demo --mib 32

image:
	docker build -t hadoop-result-writer:prototype .

integration:
	docker build -t hadoop-result-writer:integration integration
	docker run --rm --hostname hadoop --memory=3g \
		--tmpfs /tmp:rw,nosuid,size=1536m hadoop-result-writer:integration
