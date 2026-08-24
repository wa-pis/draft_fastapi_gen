# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.11.23 AS uv

FROM python:3.13 AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock Makefile ./
RUN uv sync --locked --no-dev --group build --no-install-project

COPY src ./src
RUN make build

FROM python:3.13-slim AS runtime

ENV HOME=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TMPDIR=/tmp \
    XDG_CACHE_HOME=/tmp/.cache

RUN groupadd --gid 10001 worker \
    && useradd --uid 10001 --gid worker --no-create-home --home-dir /tmp worker

WORKDIR /app

COPY --from=builder --chown=worker:worker /app/dist/calculation-worker /app/calculation-worker

USER worker
VOLUME ["/tmp"]

CMD ["/app/calculation-worker"]
