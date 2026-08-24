# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.11.23 AS uv

FROM python:3.13-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim AS runtime

ENV HOME=/tmp \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TMPDIR=/tmp \
    XDG_CACHE_HOME=/tmp/.cache

RUN groupadd --gid 10001 worker \
    && useradd --uid 10001 --gid worker --no-create-home --home-dir /tmp worker

WORKDIR /app

COPY --from=builder --chown=worker:worker /app/.venv /app/.venv

USER worker
VOLUME ["/tmp"]

CMD ["calculation-worker"]
