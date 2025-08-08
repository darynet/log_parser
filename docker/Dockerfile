FROM python:3.11-slim

WORKDIR /app

COPY ../pyproject.toml ./
RUN pip install --upgrade pip && pip install -e .[dev]

COPY ../.env ./
COPY ../app/* ./

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--limit-concurrency", "1000", "--limit-max-requests", "10000", "--backlog", "2048"]
