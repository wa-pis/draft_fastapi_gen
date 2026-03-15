#!/usr/bin/env python
"""
Interactive scaffold master for new services.

Запускать из корня репозитория:

  python scripts/scaffold_service.py

Скрипт пошагово задаёт вопросы (y/n) и поэтапно добавляет функционал:
- базовая структура сервиса
- observability (logging/metrics/tracing)
- resilience (retry/circuit breaker)
- infra (DB/Kafka/Redis/Outbox)
- пример API и health/ready
- воркер outbox publisher
"""

from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent


ROOT = Path(__file__).resolve().parents[1]


def ask_yn(question: str, default: bool = True) -> bool:
    """
    Ask a yes/no question in the terminal and return True/False.
    """
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        answer = input(f"{question} {suffix} ").strip().lower()
        if not answer:
            return default
        if answer in ("y", "yes", "д", "да"):
            return True
        if answer in ("n", "no", "н", "нет"):
            return False
        print("Please answer y or n.")


def to_service_title(service_name: str) -> str:
    return " ".join(part.capitalize() for part in service_name.replace("-", "_").split("_"))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_file(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    if path.exists():
        print(f"  skip (already exists): {path}")
        return
    path.write_text(dedent(content).lstrip(), encoding="utf-8")
    print(f"  created: {path}")


def scaffold_base(service_name: str) -> None:
    print("Step 1: базовая структура сервиса")
    if not ask_yn("Создать базовый каркас сервиса?", default=True):
        print("  пропущено.")
        return

    service_root = ROOT / "services" / service_name / "app"
    service_title = to_service_title(service_name)

    # config
    write_file(
        service_root / "config" / "settings.py",
        f"""
        from functools import lru_cache
        from pydantic import BaseSettings, AnyUrl


        class Settings(BaseSettings):
            SERVICE_NAME: str = "{service_name}"
            ENV: str = "local"
            DEBUG: bool = True

            HOST: str = "0.0.0.0"
            PORT: int = 8000

            DATABASE_URL: AnyUrl
            KAFKA_BOOTSTRAP_SERVERS: str
            KAFKA_CLIENT_ID: str = "{service_name}"
            REDIS_URL: AnyUrl

            OTLP_ENDPOINT: str | None = None
            LOG_LEVEL: str = "INFO"

            class Config:
                env_file = ".env"
                env_file_encoding = "utf-8"


        @lru_cache
        def get_settings() -> Settings:
            return Settings()
        """,
    )

    # FastAPI entrypoint (observable details wired later in other steps)
    write_file(
        service_root / "main.py",
        f"""
        import time
        import uuid

        from fastapi import FastAPI, Request

        from app.config.settings import get_settings


        settings = get_settings()

        app = FastAPI(title="{service_title}", version="1.0.0")


        @app.middleware("http")
        async def correlation_middleware(request: Request, call_next):
            correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
            request_id = str(uuid.uuid4())

            start = time.perf_counter()
            response = await call_next(request)
            _ = time.perf_counter() - start

            response.headers["x-correlation-id"] = correlation_id
            response.headers["x-request-id"] = request_id
            return response
        """,
    )

    print("  базовый каркас создан.")


def scaffold_observability(service_name: str) -> None:
    print("Step 2: observability (logging / metrics / tracing)")
    if not ask_yn("Добавить logging, metrics и tracing?", default=True):
        print("  пропущено.")
        return

    service_root = ROOT / "services" / service_name / "app"

    write_file(
        service_root / "observability" / "logging.py",
        """
        import json
        import logging
        import sys
        from typing import Any

        from app.config.settings import Settings


        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
                data: dict[str, Any] = {
                    "timestamp": self.formatTime(record),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                for field in ("correlation_id", "request_id", "trace_id", "span_id"):
                    value = getattr(record, field, None)
                    if value is not None:
                        data[field] = value
                return json.dumps(data, ensure_ascii=False)


        def setup_logging(settings: Settings) -> None:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(JsonFormatter())

            root = logging.getLogger()
            root.setLevel(settings.LOG_LEVEL)
            root.handlers.clear()
            root.addHandler(handler)
        """,
    )

    write_file(
        service_root / "observability" / "tracing.py",
        """
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        from app.config.settings import Settings


        def setup_tracing(settings: Settings) -> None:
            if not settings.OTLP_ENDPOINT:
                return

            resource = Resource.create({"service.name": settings.SERVICE_NAME})
            provider = TracerProvider(resource=resource)
            exporter = OTLPSpanExporter(endpoint=settings.OTLP_ENDPOINT)
            processor = BatchSpanProcessor(exporter)
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)
        """,
    )

    write_file(
        service_root / "observability" / "metrics.py",
        """
        from prometheus_client import Counter, Histogram, Gauge
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from fastapi import APIRouter, Response


        REQUEST_LATENCY = Histogram(
            "http_request_latency_seconds",
            "HTTP request latency",
            ["method", "path", "status"],
        )
        ERROR_COUNT = Counter(
            "http_errors_total",
            "HTTP error count",
            ["method", "path", "status"],
        )
        WORKER_THROUGHPUT = Counter(
            "worker_messages_processed_total",
            "Number of messages processed by workers",
            ["worker"],
        )
        DB_QUERY_LATENCY = Histogram(
            "db_query_latency_seconds",
            "Database query latency",
            ["operation"],
        )
        KAFKA_LAG = Gauge(
            "kafka_consumer_lag",
            "Kafka consumer lag",
            ["group", "topic", "partition"],
        )


        router = APIRouter()


        @router.get("/metrics")
        async def metrics() -> Response:
            data = generate_latest()
            return Response(content=data, media_type=CONTENT_TYPE_LATEST)


        def observe_request(method: str, path: str, status: int, duration: float) -> None:
            REQUEST_LATENCY.labels(method=method, path=path, status=status).observe(duration)
            if status >= 400:
                ERROR_COUNT.labels(method=method, path=path, status=status).inc()
        """,
    )

    # wire observability into main.py if present
    main_path = service_root / "main.py"
    if main_path.exists():
        content = main_path.read_text(encoding="utf-8")
        if "setup_logging" not in content:
            content += dedent(
                """

                from app.observability.logging import setup_logging
                from app.observability.metrics import router as metrics_router
                from app.observability.tracing import setup_tracing


                setup_logging(settings)
                setup_tracing(settings)
                app.include_router(metrics_router)
                """
            )
            main_path.write_text(content, encoding="utf-8")
            print("  main.py updated with observability wiring.")


def scaffold_resilience(service_name: str) -> None:
    print("Step 3: resilience (retry / circuit breaker)")
    if not ask_yn("Добавить retry и circuit breaker утилиты?", default=True):
        print("  пропущено.")
        return

    service_root = ROOT / "services" / service_name / "app"

    write_file(
        service_root / "resilience" / "retry.py",
        """
        import asyncio
        import random
        from collections.abc import Callable, Awaitable
        from typing import TypeVar


        T = TypeVar("T")


        async def retry_with_backoff(
            func: Callable[..., Awaitable[T]],
            *args,
            retries: int = 5,
            base_delay: float = 0.1,
            max_delay: float = 5.0,
            jitter: bool = True,
            **kwargs,
        ) -> T:
            attempt = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception:
                    attempt += 1
                    if attempt > retries:
                        raise
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    if jitter:
                        delay = random.uniform(0, delay)
                    await asyncio.sleep(delay)
        """,
    )

    write_file(
        service_root / "resilience" / "circuit_breaker.py",
        """
        import time
        from enum import Enum
        from collections.abc import Awaitable, Callable
        from typing import TypeVar


        T = TypeVar("T")


        class CircuitState(str, Enum):
            CLOSED = "closed"
            OPEN = "open"
            HALF_OPEN = "half_open"


        class CircuitBreaker:
            def __init__(self, failure_threshold: int = 5, reset_timeout: float = 30.0) -> None:
                self.state = CircuitState.CLOSED
                self.failure_threshold = failure_threshold
                self.reset_timeout = reset_timeout
                self._failure_count = 0
                self._opened_at: float | None = None

            async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
                if self.state is CircuitState.OPEN:
                    assert self._opened_at is not None
                    if time.time() - self._opened_at > self.reset_timeout:
                        self.state = CircuitState.HALF_OPEN
                    else:
                        raise RuntimeError("Circuit breaker open")

                try:
                    result = await func(*args, **kwargs)
                except Exception:
                    self._failure_count += 1
                    if self._failure_count >= self.failure_threshold:
                        self.state = CircuitState.OPEN
                        self._opened_at = time.time()
                    raise
                else:
                    if self.state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
                        self.state = CircuitState.CLOSED
                    self._failure_count = 0
                    return result
        """,
    )


def scaffold_infra(service_name: str) -> None:
    print("Step 4: infra (DB / Kafka / Redis / Outbox)")
    if not ask_yn("Добавить адаптеры DB, Kafka, Redis и outbox?", default=True):
        print("  пропущено.")
        return

    service_root = ROOT / "services" / service_name / "app"

    write_file(
        service_root / "infra" / "db.py",
        """
        from contextlib import asynccontextmanager

        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        from app.config.settings import get_settings


        settings = get_settings()
        engine = create_async_engine(str(settings.DATABASE_URL), pool_pre_ping=True)
        SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


        @asynccontextmanager
        async def get_db_session() -> AsyncSession:
            async with SessionLocal() as session:
                try:
                    yield session
                finally:
                    await session.close()


        async def db_is_healthy() -> bool:
            async with SessionLocal() as session:
                try:
                    await session.execute("SELECT 1")  # type: ignore[arg-type]
                    return True
                except Exception:
                    return False
        """,
    )

    write_file(
        service_root / "infra" / "kafka.py",
        """
        from __future__ import annotations

        from typing import Any

        from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

        from app.config.settings import get_settings
        from app.resilience.circuit_breaker import CircuitBreaker
        from app.resilience.retry import retry_with_backoff


        settings = get_settings()


        class KafkaProducer:
            def __init__(self) -> None:
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    client_id=settings.KAFKA_CLIENT_ID,
                )
                self._cb = CircuitBreaker()

            async def start(self) -> None:
                await self._producer.start()

            async def stop(self) -> None:
                await self._producer.stop()

            async def send(
                self,
                topic: str,
                value: bytes,
                headers: list[tuple[str, bytes]] | None = None,
            ) -> None:
                async def _send() -> Any:
                    return await self._producer.send_and_wait(topic, value=value, headers=headers)

                await self._cb.call(lambda: retry_with_backoff(_send))


        def build_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
            return AIOKafkaConsumer(
                topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=group_id,
                enable_auto_commit=False,
            )


        async def kafka_is_healthy() -> bool:
            producer = KafkaProducer()
            try:
                await producer.start()
                return True
            except Exception:
                return False
            finally:
                try:
                    await producer.stop()
                except Exception:
                    pass
        """,
    )

    write_file(
        service_root / "infra" / "redis_client.py",
        """
        from redis.asyncio import Redis

        from app.config.settings import get_settings


        settings = get_settings()
        redis = Redis.from_url(str(settings.REDIS_URL), decode_responses=True)


        async def redis_is_healthy() -> bool:
            try:
                await redis.ping()
                return True
            except Exception:
                return False
        """,
    )

    write_file(
        service_root / "infra" / "outbox.py",
        f"""
        from datetime import datetime

        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import AsyncSession

        from app.domain.events import DomainEvent


        async def add_outbox_event(session: AsyncSession, event: DomainEvent) -> None:
            await session.execute(
                text(
                    '''
                    INSERT INTO outbox_events
                        (aggregate_type, aggregate_id, event_type, payload, headers, occurred_at, status)
                    VALUES
                        (:aggregate_type, :aggregate_id, :event_type, :payload, :headers, :occurred_at, 'pending')
                    '''
                ),
                {{
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": event.aggregate_id,
                    "event_type": event.event_type,
                    "payload": event.payload,
                    "headers": {{"topic": "{service_name}.v1.events", "event_id": event.id}},
                    "occurred_at": event.occurred_at,
                }},
            )


        async def fetch_pending_batch(session: AsyncSession, limit: int = 100) -> list[dict]:
            result = await session.execute(
                text(
                    '''
                    SELECT id, aggregate_type, aggregate_id, event_type, payload, headers
                    FROM outbox_events
                    WHERE status = 'pending'
                    ORDER BY id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                    '''
                ),
                {{"limit": limit}},
            )
            rows = result.mappings().all()
            ids = [row["id"] for row in rows]
            if ids:
                await session.execute(
                    text("UPDATE outbox_events SET status = 'publishing' WHERE id = ANY(:ids)"),
                    {{"ids": ids}},
                )
            return [dict(row) for row in rows]


        async def mark_published(session: AsyncSession, event_id: int) -> None:
            await session.execute(
                text(
                    '''
                    UPDATE outbox_events
                    SET status = 'published', published_at = :now
                    WHERE id = :id
                    '''
                ),
                {{"id": event_id, "now": datetime.utcnow()}},
            )


        async def mark_failed(session: AsyncSession, event_id: int, error: str) -> None:
            await session.execute(
                text(
                    '''
                    UPDATE outbox_events
                    SET status = 'failed', error = :error
                    WHERE id = :id
                    '''
                ),
                {{"id": event_id, "error": error}},
            )
        """,
    )

    # domain events base (if not yet)
    write_file(
        service_root / "domain" / "events.py",
        """
        from dataclasses import dataclass, asdict
        from datetime import datetime
        from typing import Any
        from uuid import uuid4


        @dataclass
        class DomainEvent:
            id: str
            event_type: str
            aggregate_type: str
            aggregate_id: str
            occurred_at: datetime
            payload: dict[str, Any]

            def to_dict(self) -> dict[str, Any]:
                return asdict(self)


        def new_event(
            event_type: str,
            aggregate_type: str,
            aggregate_id: str,
            payload: dict[str, Any],
        ) -> DomainEvent:
            return DomainEvent(
                id=str(uuid4()),
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                occurred_at=datetime.utcnow(),
                payload=payload,
            )
        """,
    )


def scaffold_api_example(service_name: str) -> None:
    print("Step 5: пример API и health/ready")
    if not ask_yn("Добавить пример роутов (/health, /ready, /v1/ping)?", default=True):
        print("  пропущено.")
        return

    service_root = ROOT / "services" / service_name / "app"

    write_file(
        service_root / "application" / "idempotency.py",
        """
        import json

        from app.infra.redis_client import redis


        class IdempotencyService:
            def __init__(self, prefix: str = "idem:api:", ttl_seconds: int = 3600) -> None:
                self._prefix = prefix
                self._ttl = ttl_seconds

            async def get_existing(self, key: str | None) -> dict | None:
                if not key:
                    return None
                raw = await redis.get(self._prefix + key)
                return json.loads(raw) if raw else None

            async def store(self, key: str | None, response: dict) -> None:
                if not key:
                    return
                await redis.set(self._prefix + key, json.dumps(response), ex=self._ttl)
        """,
    )

    write_file(
        service_root / "api" / "routes.py",
        f"""
        import time

        from fastapi import APIRouter, Depends, Header, HTTPException, status
        from sqlalchemy.ext.asyncio import AsyncSession

        from app.application.idempotency import IdempotencyService
        from app.infra.db import get_db_session, db_is_healthy
        from app.infra.kafka import kafka_is_healthy
        from app.infra.redis_client import redis_is_healthy
        from app.observability.metrics import observe_request


        router = APIRouter()


        async def get_session() -> AsyncSession:
            async with get_db_session() as session:
                yield session


        @router.get("/health")
        async def health() -> dict[str, str]:
            return {{"status": "ok"}}


        @router.get("/ready")
        async def ready() -> dict[str, str]:
            if not await db_is_healthy():
                raise HTTPException(status_code=503, detail="db unavailable")
            if not await kafka_is_healthy():
                raise HTTPException(status_code=503, detail="kafka unavailable")
            if not await redis_is_healthy():
                raise HTTPException(status_code=503, detail="redis unavailable")
            return {{"status": "ready"}}


        @router.get("/v1/ping")
        async def ping(
            idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
            session: AsyncSession = Depends(get_session),
        ) -> dict[str, str]:
            idem = IdempotencyService()
            existing = await idem.get_existing(idempotency_key)
            if existing:
                return existing

            started = time.perf_counter()
            await session.execute("SELECT 1")  # type: ignore[arg-type]
            duration = time.perf_counter() - started
            observe_request("GET", "/v1/ping", 200, duration)

            response = {{"message": "pong", "service": "{service_name}"}}
            await idem.store(idempotency_key, response)
            return response
        """,
    )

    # wire API and metrics router into main
    main_path = service_root / "main.py"
    if main_path.exists():
        content = main_path.read_text(encoding="utf-8")
        if "from app.api.routes import router as api_router" not in content:
            content += dedent(
                """

                from app.api.routes import router as api_router
                from app.observability.metrics import router as metrics_router


                app.include_router(api_router)
                app.include_router(metrics_router)
                """
            )
            main_path.write_text(content, encoding="utf-8")
            print("  main.py updated with API routing.")


def scaffold_outbox_worker(service_name: str) -> None:
    print("Step 6: воркер outbox publisher")
    if not ask_yn("Добавить воркер, публикующий события из outbox в Kafka?", default=True):
        print("  пропущено.")
        return

    service_root = ROOT / "services" / service_name / "app"

    write_file(
        service_root / "workers" / "outbox_publisher.py",
        """
        import asyncio
        import json

        from sqlalchemy.ext.asyncio import AsyncSession

        from app.infra.db import get_db_session
        from app.infra.kafka import KafkaProducer
        from app.infra.outbox import fetch_pending_batch, mark_published, mark_failed
        from app.observability.metrics import WORKER_THROUGHPUT


        BATCH_SIZE = 100
        TOPIC_HEADER = "topic"


        async def publish_once(session: AsyncSession, producer: KafkaProducer) -> None:
            events = await fetch_pending_batch(session, BATCH_SIZE)
            for row in events:
                headers = row["headers"] or {}
                topic = headers.get(TOPIC_HEADER)
                if not topic:
                    await mark_failed(session, row["id"], "missing topic header")
                    continue
                try:
                    payload_bytes = json.dumps(row["payload"]).encode("utf-8")
                    await producer.send(
                        topic,
                        payload_bytes,
                        headers=[("event_id", str(row["id"]).encode())],
                    )
                    await mark_published(session, row["id"])
                    WORKER_THROUGHPUT.labels(worker="outbox_publisher").inc()
                except Exception as exc:  # noqa: BLE001
                    await mark_failed(session, row["id"], str(exc))
            await session.commit()


        async def run() -> None:
            producer = KafkaProducer()
            await producer.start()
            try:
                while True:
                    async with get_db_session() as session:
                        await publish_once(session, producer)
                    await asyncio.sleep(0.5)
            finally:
                await producer.stop()


        if __name__ == "__main__":
            asyncio.run(run())
        """,
    )


def main() -> None:
    print("=== Service scaffold master ===")
    service_name = input("Введите имя сервиса (например, user_service): ").strip()
    if not service_name:
        print("Имя сервиса обязательно.")
        sys.exit(1)

    print(f"Будет создан сервис: services/{service_name}/app")

    scaffold_base(service_name)
    scaffold_observability(service_name)
    scaffold_resilience(service_name)
    scaffold_infra(service_name)
    scaffold_api_example(service_name)
    scaffold_outbox_worker(service_name)

    print("\nГотово.")
    print("Проверь созданные файлы и добавь доменную модель/роуты под свои задачи.")


if __name__ == "__main__":
    main()

