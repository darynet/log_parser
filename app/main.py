import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
import time
from typing import Any, AsyncGenerator
from clickhouse_driver import Client
from fastapi import FastAPI, HTTPException, Query, Request, Response
from contextlib import asynccontextmanager
import logging
from models import MessageModel
from config import settings
from kafka_service import KafkaService
import sentry_sdk
from metrics import (
    http_requests_total,
    http_request_duration_seconds,
    kafka_messages_sent_total,
    kafka_messages_consumed_total,
    clickhouse_inserts_total,
    clickhouse_insert_duration_seconds,
    batch_buffer_size,
    errors_total,
    kafka_send_duration,
    start_system_metrics_collector,
)
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST


sentry_sdk.init(
    dsn=settings.sentry_dsn,
    send_default_pii=True,
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
    profile_session_sample_rate=1.0,
    profile_lifecycle="trace",
)

logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)

kafka_service: KafkaService = KafkaService()
thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=3)
clickhouse_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=3)

batch_buffer: list[tuple[int, str, str, int, datetime]] = []
buffer_lock: Lock = Lock()

client: Client = Client(
    host=settings.clickhouse_host,
    port=settings.clickhouse_port,
    user=settings.clickhouse_user,
    password=settings.clickhouse_password,
)


def clickhouse_insert_batch(batch_data: list) -> None:
    start_time = time.time()
    logger.info(f"Начинаем вставку {len(batch_data)} записей в ClickHouse")
    try:
        logger.info(f"Выполняем INSERT в таблицу {settings.clickhouse_table}")
        client.execute(
            f"INSERT INTO {settings.clickhouse_table} (motocycle_id, motocycle_model, motocycle_number, motocycle_speed, date) VALUES",
            batch_data,
        )
        clickhouse_inserts_total.inc()
        clickhouse_insert_duration_seconds.observe(time.time() - start_time)
        logger.info(f"✅ Вставлено {len(batch_data)} сообщений в ClickHouse.")
    except Exception as e:
        errors_total.labels(type="kafka_processing").inc()
        logger.error(f"❌ Ошибка при batch-вставке в ClickHouse: {e}")
        logger.error(f"Детали ошибки: {type(e).__name__}: {str(e)}")


def handle_kafka_message(message: Any) -> None:
    # try:
    logger.info(f"Получено сообщение из топика {message.topic}: {message.value}")
    kafka_messages_consumed_total.inc()

    try:
        dt: datetime = datetime.fromisoformat(message.value["date"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    except TypeError as e:
        logger.error(f"Ошибка преобразования date {message.value['date']}: {e}")
        return
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения {message}:\n{e}")
        return

    row: tuple[int, str, str, int, datetime] = (
        message.value["motocycle_id"],
        message.value["motocycle_model"],
        message.value["motocycle_number"],
        message.value["motocycle_speed"],
        dt,
    )

    logger.info(f"Сообщение: {row}")
    with buffer_lock:
        batch_buffer.append(row)
        batch_buffer_size.set(len(batch_buffer))
        logger.info(f"Добавлено в буфер. Размер буфера: {len(batch_buffer)}/{settings.batch_size}")
        if len(batch_buffer) >= int(settings.batch_size):
            thread_pool.submit(clickhouse_insert_batch, batch_buffer.copy())
            batch_buffer.clear()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    start_system_metrics_collector()

    kafka_service.start_consumer(handle_kafka_message)

    yield

    with buffer_lock:
        if batch_buffer:
            logger.info(f"Вставляем оставшиеся {len(batch_buffer)} записей из буфера")
            thread_pool.submit(clickhouse_insert_batch, batch_buffer.copy())
            batch_buffer.clear()

    kafka_service.stop_consumer()


app: FastAPI = FastAPI(title=settings.app_name, version=settings.app_version, lifespan=lifespan)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    start_time = time.time()

    response = await call_next(request)

    duration = time.time() - start_time

    http_requests_total.labels(method=request.method, endpoint=request.url.path, status=response.status_code).inc()

    http_request_duration_seconds.labels(method=request.method, endpoint=request.url.path).observe(duration)

    return response


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": f"{settings.app_name} работает"}


@app.get("/health")
async def health_check() -> dict[str, str | dict[str, str] | int]:
    try:
        kafka_status = "running" if kafka_service.is_consumer_running() else "stopped"

        clickhouse_status = "healthy"
        try:
            client.execute("SELECT 1")
        except Exception:
            clickhouse_status = "unhealthy"

        buffer_size = len(batch_buffer)

        return {
            "status": "healthy",
            "kafka_consumer": kafka_status,
            "clickhouse": clickhouse_status,
            "buffer_size": buffer_size,
            "version": settings.app_version,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "version": settings.app_version,
        }


@app.get("/metrics")
async def metrics():
    """Эндпоинт для Prometheus метрик"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/send-message")
async def send_message(message: MessageModel, topic: str = settings.kafka_topic) -> dict[str, str]:
    """Отправка сообщения в Kafka"""
    start_time = time.time()
    try:
        kafka_messages_sent_total.inc()
        logger.info(f"Отправка сообщения в Kafka: {message.model_dump()}")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(thread_pool, kafka_service.send_message, topic, message.model_dump())
        kafka_send_duration.observe(time.time() - start_time)
        return {"status": "success", "message": "Сообщение отправлено"}
    except Exception as e:
        errors_total.labels(type="kafka_send").inc()
        raise HTTPException(status_code=500, detail=f"Ошибка отправки: {str(e)}")


@app.get("/get_logs")
async def get_logs(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> list[dict]:
    try:
        rows = client.execute(
            f"SELECT motocycle_id, motocycle_model, motocycle_number, motocycle_speed, date FROM {settings.clickhouse_table} ORDER BY date DESC LIMIT {limit} OFFSET {offset}",
        )
        if rows is None or not isinstance(rows, (list, tuple)):
            return []

        result = []
        for row in rows:
            try:
                result.append(
                    {
                        "motocycle_id": row[0],
                        "motocycle_model": row[1],
                        "motocycle_number": row[2],
                        "motocycle_speed": row[3],
                        "date": row[4].isoformat() if hasattr(row[4], "isoformat") else str(row[4]),
                    }
                )
            except (IndexError, TypeError) as e:
                logger.error(f"Ошибка обработки строки {row}: {e}")
                continue

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при запросе логов из ClickHouse: {e}")
