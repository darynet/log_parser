FROM python:3.11-slim

WORKDIR /app

COPY ../pyproject.toml ./
RUN pip install --upgrade pip && pip install -e .[dev]

COPY ../.env ./
COPY ../app/main.py ./
COPY ../app/config.py ./
COPY ../app/kafka_service.py ./

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
