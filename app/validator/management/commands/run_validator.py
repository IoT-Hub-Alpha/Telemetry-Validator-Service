"""
Django management command to run the telemetry validator consumer loop.

Usage: python manage.py run_validator
"""

import logging
import signal

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from IoTKafka import IoTKafkaConsumer, IoTKafkaProducer
from confluent_kafka import KafkaError

from validator.services.pipeline import MessageRouter
from validator.services.schema_cache import SchemaCache

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Consume telemetry.raw, validate, transform, and route to clean/dlq."

    def __init__(self):
        super().__init__()
        self._running = True

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=settings.CONSUMER_BATCH_SIZE,
            help="Number of messages to consume per batch",
        )
        parser.add_argument(
            "--poll-timeout",
            type=float,
            default=settings.CONSUMER_POLL_TIMEOUT,
            help="Timeout in seconds for consumer.consume()",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        poll_timeout = options["poll_timeout"]

        logger.info("Starting Telemetry Validator Service")

        self._install_signal_handlers()

        schema_cache = SchemaCache()
        schema_cache.warm_cache()

        producer = IoTKafkaProducer()
        consumer = IoTKafkaConsumer(
            group_id=settings.KAFKA_CONSUMER_GROUP,
            enable_auto_offset_store=True,
        )
        consumer.subscribe_topics([settings.KAFKA_TOPIC_TELEMETRY_RAW])

        router = MessageRouter(schema_cache, producer)

        processed_total = 0

        self.stdout.write(
            self.style.SUCCESS(
                f"Worker started (group={settings.KAFKA_CONSUMER_GROUP}, "
                f"batch_size={batch_size})"
            )
        )

        try:
            while self._running:
                messages = consumer.consume(
                    num_messages=batch_size,
                    timeout=poll_timeout,
                )

                if not messages:
                    continue

                batch_processed = 0
                for message in messages:
                    if message.error():
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            logger.error(
                                "kafka_consume_error",
                                extra={"error": str(message.error())},
                            )
                        continue

                    router.process_message(message)
                    batch_processed += 1
                    processed_total += 1

                if batch_processed == 0:
                    continue

                producer.flush(timeout=30)

                consumer.commit(asynchronous=False)

                logger.info(
                    "Batch processed and committed",
                    extra={
                        "batch_size": batch_processed,
                        "total": processed_total,
                    },
                )

        except Exception:
            logger.exception("Fatal error in consumer loop")
            raise CommandError("Consumer loop crashed")
        finally:
            producer.flush(timeout=30)
            consumer.close()
            schema_cache.close()
            self.stdout.write(
                self.style.SUCCESS(
                    f"Worker stopped. Total processed: {processed_total}"
                )
            )

    def _install_signal_handlers(self) -> None:
        def _stop(signum, frame):
            logger.info("Received signal %s, shutting down...", signum)
            self._running = False

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
