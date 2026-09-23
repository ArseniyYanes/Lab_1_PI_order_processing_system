"""Shared environment configuration for all Python services.

All services in the docker-compose network read their settings from
environment variables (set in docker-compose.yml), so the code stays
portable between local runs and containers.
"""
import os


# --- MySQL ---
def mysql_config():
    return {
        "host": os.environ.get("MYSQL_HOST", "mysql"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "app"),
        "password": os.environ.get("MYSQL_PASSWORD", "apppass"),
        "database": os.environ.get("MYSQL_DATABASE", "orders_db"),
        "charset": "utf8mb4",
    }


# --- Kafka ---
def kafka_bootstrap():
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "order-events")

# --- RabbitMQ ---
def rabbitmq_url():
    return os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")


RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "order-notifications")
