import psutil
import time
from prometheus_client import Gauge, Counter, Histogram
from threading import Thread

# Метрики для HTTP запросов
http_requests_total = Counter("http_requests_total", "Total number of HTTP requests", ["method", "endpoint", "status"])

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "HTTP request duration in seconds", ["method", "endpoint"]
)

# Метрики для Kafka
kafka_messages_sent_total = Counter("kafka_messages_sent_total", "Total number of messages sent to Kafka")

kafka_messages_consumed_total = Counter("kafka_messages_consumed_total", "Total number of messages consumed from Kafka")

kafka_send_duration = Histogram("kafka_send_duration_seconds", "Kafka send duration in seconds")

# Метрики для ClickHouse
clickhouse_inserts_total = Counter("clickhouse_inserts_total", "Total number of batch inserts to ClickHouse")

clickhouse_insert_duration_seconds = Histogram(
    "clickhouse_insert_duration_seconds", "ClickHouse batch insert duration in seconds"
)

# Метрики для буфера
batch_buffer_size = Gauge("batch_buffer_size", "Current size of the batch buffer")

# Метрики для ошибок
errors_total = Counter("errors_total", "Total number of errors", ["type"])

# Метрики для системных ресурсов
cpu_usage_percent = Gauge("cpu_usage_percent", "CPU usage percentage")
memory_usage_bytes = Gauge("memory_usage_bytes", "Memory usage in bytes")
memory_usage_percent = Gauge("memory_usage_percent", "Memory usage percentage")
disk_usage_bytes = Gauge("disk_usage_bytes", "Disk usage in bytes")
disk_usage_percent = Gauge("disk_usage_percent", "Disk usage percentage")

# Метрики для процесса
process_cpu_percent = Gauge("process_cpu_percent", "Process CPU usage percentage")
process_memory_bytes = Gauge("process_memory_bytes", "Process memory usage in bytes")
process_memory_percent = Gauge("process_memory_percent", "Process memory usage percentage")


def collect_system_metrics():
    """Сбор системных метрик"""
    while True:
        try:
            # Системные метрики
            cpu_usage_percent.set(psutil.cpu_percent(interval=1))

            memory = psutil.virtual_memory()
            memory_usage_bytes.set(memory.used)
            memory_usage_percent.set(memory.percent)

            disk = psutil.disk_usage("/")
            disk_usage_bytes.set(disk.used)
            disk_usage_percent.set((disk.used / disk.total) * 100)

            # Метрики процесса
            process = psutil.Process()
            process_cpu_percent.set(process.cpu_percent())
            process_memory_bytes.set(process.memory_info().rss)
            process_memory_percent.set(process.memory_percent())

        except Exception as e:
            print(f"Ошибка сбора системных метрик: {e}")

        time.sleep(15)  # Обновляем каждые 15 секунд


def start_system_metrics_collector():
    """Запуск сбора системных метрик в отдельном потоке"""
    thread = Thread(target=collect_system_metrics, daemon=True)
    thread.start()
    return thread
