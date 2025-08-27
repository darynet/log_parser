from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from typing import Callable
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from config import settings

logger: logging.Logger = logging.getLogger(__name__)


class KafkaService:
    def __init__(self) -> None:
        # Получаем настройки из переменных окружения
        self.bootstrap_servers: list[str] = settings.kafka_bootstrap_servers.split(",")
        self.topic: str = settings.kafka_topic
        self.group_id: str = settings.kafka_group_id
        self.auto_offset_reset: str = settings.kafka_auto_offset_reset
        self.consumer_count: int = int(settings.kafka_consumer_count)

        self.producer: KafkaProducer | None = None
        self.consumer_pool: ThreadPoolExecutor | None = None
        self.stop_event: threading.Event = threading.Event()

    def create_consumer(self, consumer_id: int = 0) -> KafkaConsumer:
        # Создаем уникальный group_id для каждого consumer'а
        group_id = f"{self.group_id}-{consumer_id}" if self.consumer_count > 1 else self.group_id

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
            consumer_timeout_ms=5000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )

        logger.info(f"Consumer {consumer_id} подписывается на топик: {self.topic} с group_id: {group_id}")
        consumer.subscribe([self.topic])

        return consumer

    def create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
            acks=1,
            retries=3,
            batch_size=32768,
            linger_ms=1,
            buffer_memory=67108864,
            max_in_flight_requests_per_connection=5,
            request_timeout_ms=30000,
            compression_type="lz4",
            max_request_size=1048576,
        )

    def start_consumer(self, message_handler: Callable) -> None:
        if self.consumer_pool:
            logger.warning("Consumer(ы) уже запущены")
            return

        logger.info(f"Запускаем Kafka consumer для топика: {self.topic}")
        self.stop_event.clear()
        self.consumer_pool = ThreadPoolExecutor(max_workers=self.consumer_count)

        for i in range(self.consumer_count):
            logger.info(f"Запускаем consumer поток {i + 1}")
            future = self.consumer_pool.submit(self._consume_messages, message_handler, i)
            future.add_done_callback(lambda f, thread_num=i + 1: self._on_consumer_done(f, thread_num))

        logger.info(f"Kafka consumer запущен в {self.consumer_count} потоках")

    def _on_consumer_done(self, future, thread_num: int) -> None:
        """Callback для отслеживания завершения consumer потока"""
        try:
            future.result()
            logger.info(f"Consumer поток {thread_num} завершился нормально")
        except Exception as e:
            logger.error(f"Consumer поток {thread_num} завершился с ошибкой: {e}")

    def stop_consumer(self) -> None:
        if self.consumer_pool:
            logger.info("Остановка Kafka consumer(ов)...")
            self.stop_event.set()
            self.consumer_pool.shutdown(wait=True)
            self.consumer_pool = None
            logger.info("Kafka consumer(ы) остановлены")

    def _consume_messages(self, message_handler: Callable, consumer_id: int) -> None:
        consumer: KafkaConsumer | None = None
        try:
            logger.info(f"Consumer {consumer_id} создает подключение к топику {self.topic}")
            consumer = self.create_consumer(consumer_id)

            # Ждем назначения партиций
            logger.info(f"Consumer {consumer_id} ожидает назначения партиций...")
            assignment_timeout = 30
            start_time = time.time()

            while not consumer.assignment() and time.time() - start_time < assignment_timeout:
                consumer.poll(timeout_ms=1000)
                time.sleep(0.1)

            if not consumer.assignment():
                logger.error(f"Consumer {consumer_id}: Партиции не были назначены в течение таймаута")
                return

            logger.info(f"Consumer {consumer_id} назначены партиции: {consumer.assignment()}")
            logger.info(f"Consumer {consumer_id} начинает основной цикл потребления сообщений")

            while not self.stop_event.is_set():
                try:
                    message_batch = consumer.poll(timeout_ms=5000, max_records=100)

                    if not message_batch:
                        continue

                    total_messages = sum(len(messages) for messages in message_batch.values())
                    logger.info(f"Consumer {consumer_id} получил {total_messages} сообщений")

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if self.stop_event.is_set():
                                break
                            try:
                                message_handler(message)
                            except Exception as e:
                                logger.error(f"Consumer {consumer_id}: Ошибка в message_handler: {e}")

                except Exception as e:
                    logger.error(f"Consumer {consumer_id}: Ошибка при получении сообщений: {e}")
                    time.sleep(5)

        except Exception as e:
            logger.error(f"Consumer {consumer_id}: Критическая ошибка: {e}")
        finally:
            if consumer:
                logger.info(f"Consumer {consumer_id} закрывает подключение")
                try:
                    consumer.close()
                except Exception as e:
                    logger.error(f"Consumer {consumer_id}: Ошибка при закрытии: {e}")

    def send_message(self, topic: str, message: dict) -> None:
        if not self.producer:
            logger.info("Создаем Kafka producer")
            self.producer = self.create_producer()

        try:
            logger.info(f"Отправляем сообщение в топик {topic}")
            future = self.producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            logger.info(
                f"✅ Сообщение отправлено: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}"
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}")
            raise

    def is_consumer_running(self) -> bool:
        return self.consumer_pool is not None
