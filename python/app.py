"""Flask service: accepts orders (POST /api/orders).

Step 5: for each order we
  1. store it in MySQL (table `orders`) and get the generated order_id;
  2. publish an event to the Kafka topic `order-events`, using order_id
     as the message key so events of one order stay on one partition.

The RabbitMQ notification (step 6) will be added to this same handler.
"""
import json
import logging

import pymysql
from flask import Flask, jsonify, request
from kafka import KafkaProducer

import env

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
logger = logging.getLogger("flask-orders")

app = Flask(__name__)

# One shared, long-lived Kafka producer, created lazily on first request.
_producer = None


def _get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=env.kafka_bootstrap(),
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=5,
        )
        logger.info("Kafka producer ready -> %s (topic=%s)", env.kafka_bootstrap(), env.KAFKA_TOPIC)
    return _producer


@app.route("/api/health", methods=["GET"])
def health():
    """Health endpoint for checks / debugging (e.g. curl from nginx)."""
    return jsonify(status="ok", service="flask-orders")


@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json(silent=True) or {}
    buyer = (data.get("buyer") or "").strip()
    product = (data.get("product") or "").strip()
    raw_amount = data.get("amount")

    if not buyer or not product or raw_amount in (None, ""):
        return jsonify(error="fields 'buyer', 'product' and 'amount' are required"), 400
    try:
        amount = round(float(raw_amount), 2)
    except (TypeError, ValueError):
        return jsonify(error="'amount' must be a number"), 400

    # 1) Store the order in MySQL and get the generated order_id.
    conn = pymysql.connect(**env.mysql_config())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (buyer, product, amount) VALUES (%s, %s, %s)",
                (buyer, product, amount),
            )
            order_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()
    logger.info("order #%s stored in MySQL (buyer=%s product=%s amount=%.2f)", order_id, buyer, product, amount)

    # 2) Publish the order event to Kafka, using order_id as the message key.
    event = {
        "order_id": order_id,
        "buyer": buyer,
        "product": product,
        "amount": amount,
    }
    try:
        record = _get_producer().send(env.KAFKA_TOPIC, key=order_id, value=event)
        record.get(timeout=10)
        logger.info("order event published to %s (key=%s)", env.KAFKA_TOPIC, order_id)
    except Exception as exc:  # order is already safely in MySQL
        logger.exception("order saved to MySQL but Kafka publish failed")
        return jsonify(order_id=order_id, error=f"Kafka publish failed: {exc}"), 502

    return jsonify(order_id=order_id, **event), 201


if __name__ == "__main__":
    logger.info("Starting Flask orders service on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000)
