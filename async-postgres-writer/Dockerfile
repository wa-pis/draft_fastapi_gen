# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.11.23 AS uv
FROM python:3.13-slim AS builder

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /app
COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md ./
COPY openapi ./openapi
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 writer \
    && useradd --uid 10001 --gid writer --no-create-home writer

WORKDIR /app
COPY --from=builder --chown=writer:writer /app/.venv /app/.venv
USER 10001:10001
EXPOSE 8080

ENTRYPOINT ["/app/.venv/bin/async-postgres-writer"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8080"]
