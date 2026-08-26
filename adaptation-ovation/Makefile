.PHONY: build

build:
	PYINSTALLER_CONFIG_DIR=build/pyinstaller-cache uv run --locked --no-dev --group build pyinstaller \
		--clean \
		--noconfirm \
		--onefile \
		--name calculation-worker \
		--paths src \
		--collect-all dbos \
		--hidden-import sqlalchemy.dialects.postgresql.psycopg \
		--collect-binaries psycopg_binary \
		--distpath dist \
		--workpath build/pyinstaller \
		--specpath build/pyinstaller \
		src/calculation_worker/main.py
