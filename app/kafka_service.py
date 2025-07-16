from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from typing import Any, Callable
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from config import settings

logger: logging.Logger = logging.getLogger(__name__)


kafka_config: dict[str, str | list[str] | int] = {
    "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
    "topic": settings.kafka_topic,
    "group_id": settings.kafka_group_id,
    "auto_offset_reset": settings.kafka_auto_offset_reset,
}


class KafkaService:
    def __init__(self, config: dict[str, str | list[str] | int]) -> None:
        self.config: dict[str, str | list[str] | int] = config
        self.consumer: KafkaConsumer | None = None
        self.producer: KafkaProducer | None = None
        self.consumer_pool: ThreadPoolExecutor | None = None
        self.consumer_count: int = int(settings.kafka_consumer_count)
        self.stop_event: threading.Event = threading.Event()

    def create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.config["topic"],
            bootstrap_servers=self.config["bootstrap_servers"],
            auto_offset_reset=self.config["auto_offset_reset"],
            enable_auto_commit=True,
            group_id=self.config["group_id"],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
            consumer_timeout_ms=1000,
        )

    def create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.config["bootstrap_servers"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    def start_consumer(self, message_handler: Callable) -> None:
        if self.consumer_pool:
            logger.warning("Consumer(ы) уже запущены")
            return

        self.stop_event.clear()
        self.consumer_pool = ThreadPoolExecutor(max_workers=self.consumer_count)
        for _ in range(self.consumer_count):
            self.consumer_pool.submit(self._consume_messages, message_handler)
        logger.info(f"Kafka consumer запущен в {self.consumer_count} потоках")

    def stop_consumer(self) -> None:
        if self.consumer_pool:
            logger.info("Остановка Kafka consumer(ов)...")
            self.stop_event.set()
            self.consumer_pool.shutdown(wait=True)
            self.consumer_pool = None
            logger.info("Kafka consumer(ы) остановлены")

    def _consume_messages(self, message_handler: Callable) -> None:
        try:
            self.consumer = self.create_consumer()

            while not self.stop_event.is_set():
                try:
                    messages: dict[str, Any] = self.consumer.poll(timeout_ms=1000)

                    for _, records in messages.items():
                        for record in records:
                            if self.stop_event.is_set():
                                break
                            message_handler(record)

                except Exception as e:
                    logger.error(f"Ошибка при получении сообщений: {e}")
                    time.sleep(1)

        except Exception as e:
            logger.error(f"Критическая ошибка в consumer: {e}")
        finally:
            if self.consumer:
                self.consumer.close()

    def send_message(self, topic: str, message: dict) -> None:
        if not self.producer:
            self.producer = self.create_producer()

        try:
            future = self.producer.send(topic, message)
            future.get(timeout=10)
            logger.info(f"Сообщение отправлено в топик {topic}")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {e}")
            raise

    def is_consumer_running(self) -> bool:
        return self.consumer_pool is not None
