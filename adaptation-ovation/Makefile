.PHONY: build

build:
	uv run --locked --no-dev --group build pyinstaller \
		--clean \
		--noconfirm \
		--onefile \
		--name calculation-worker \
		--paths src \
		--distpath dist \
		--workpath build/pyinstaller \
		--specpath build/pyinstaller \
		src/calculation_worker/main.py
