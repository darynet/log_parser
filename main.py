from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Literal
from clickhouse_driver import Client
from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager
import logging
from config import settings
from kafka_service import KafkaService, kafka_config

logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)

kafka_service: KafkaService = KafkaService(kafka_config)

client: Client = Client(
    host=settings.clickhouse_host,
    port=settings.clickhouse_port,
    user=settings.clickhouse_user,
    password=settings.clickhouse_password,
)


def handle_kafka_message(message: dict[str, Any]) -> None:
    try:
        logger.info(f"Получено сообщение из топика {message.topic}: {message.value}")

        try:
            dt: datetime = datetime.fromisoformat(message.value["timestamp"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        except TypeError as e:
            logger.error(f"Ошибка преобразования timestamp {message.value['timestamp']}: {e}")
            return
        except Exception as e:
            logger.error(message)
            logger.error(f"Ошибка при обработке сообщения {message}:\n{e}")
            return

        try:
            client.execute("SHOW DATABASES")
            client.execute(
                f"INSERT INTO {settings.clickhouse_table} (message, hostname, timestamp) VALUES",
                [(message.value["message"], message.value["hostname"], dt)],
            )
            logger.info("Сообщение записано в ClickHouse.")
        except Exception as e:
            logger.error(f"Ошибка при записи в ClickHouse: {e}")

    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    kafka_service.start_consumer(handle_kafka_message)

    yield

    kafka_service.stop_consumer()


app: FastAPI = FastAPI(title=settings.app_name, version=settings.app_version, lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": f"{settings.app_name} работает"}


@app.get("/health")
async def health_check() -> dict[str, str | dict[str, str]]:
    consumer_status: Literal["running", "stopped"] = "running" if kafka_service.is_consumer_running() else "stopped"
    return {
        "status": "healthy",
        "kafka_consumer": consumer_status,
        "version": settings.app_version,
    }


@app.post("/send-message")
async def send_message(message: dict) -> dict[str, str]:
    """Отправка сообщения в Kafka"""
    try:
        kafka_service.send_message(settings.kafka_topic, message)
        return {"status": "success", "message": "Сообщение отправлено"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка отправки: {str(e)}")


@app.get("/get_logs")
async def get_logs(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> list[dict]:
    try:
        rows = client.execute(
            f"SELECT message, hostname, timestamp FROM {settings.clickhouse_table} ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}",
        )
        result = [
            {
                "message": row[0],
                "hostname": row[1],
                "timestamp": row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2]),
            }
            for row in rows
        ]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при запросе логов из ClickHouse: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
