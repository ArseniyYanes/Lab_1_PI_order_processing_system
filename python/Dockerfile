# Single image for all Python services in the lab:
#   - Flask app (POST /api/orders)            -> command: python app.py
#   - Kafka consumer: payment-group           -> command: python payment_consumer.py
#   - Kafka consumer: delivery-group          -> command: python delivery_consumer.py
#   - RabbitMQ worker: order-notifications    -> command: python rabbit_worker.py
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first (better Docker layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code (app.py + consumers + worker will be added in next commits)
COPY . .

EXPOSE 5000

# Default: Flask app. Consumer/worker services override the command in docker-compose.
CMD ["python", "app.py"]
