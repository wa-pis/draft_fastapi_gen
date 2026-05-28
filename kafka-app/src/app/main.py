import asyncio
import logging
import signal

from app.config import get_settings
from app.handlers.event_handler import EventHandler
from app.kafka.consumer import KafkaConsumer
from app.kafka.producer import KafkaProducer
from app.logging_config import configure_logging
from app.observability.tracing import configure_tracing, shutdown_tracing
from app.services.event_service import EventService

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.app, settings.logging)
    configure_tracing(settings.app, settings.otel)

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    producer = KafkaProducer(settings.kafka)
    service = EventService()
    handler = EventHandler(service)
    consumer = KafkaConsumer(settings.kafka, handler, producer)

    try:
        await producer.start()
        await consumer.start()
        consumer_task = asyncio.create_task(consumer.run(), name="kafka-consumer")

        logger.info("Service started")
        stop_task = asyncio.create_task(stop_event.wait(), name="shutdown-signal-waiter")
        done, _ = await asyncio.wait(
            {consumer_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if consumer_task in done:
            consumer_task.result()

        logger.info("Shutdown signal received")
        stop_task.cancel()
        consumer_task.cancel()
        await asyncio.gather(stop_task, consumer_task, return_exceptions=True)
    finally:
        await consumer.stop()
        await producer.stop()
        await _cancel_pending_tasks()
        shutdown_tracing()
        logging.shutdown()


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())


async def _cancel_pending_tasks() -> None:
    current_task = asyncio.current_task()
    pending = [
        task
        for task in asyncio.all_tasks()
        if task is not current_task and not task.done()
    ]
    if not pending:
        return

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
