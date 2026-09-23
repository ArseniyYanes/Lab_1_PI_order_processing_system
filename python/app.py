"""Flask service: accepts orders (POST /api/orders).

Step 2: placeholder skeleton only.
The endpoint is wired to MySQL + Kafka (step 5) and RabbitMQ (step 6).
"""
import logging

from flask import Flask, jsonify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flask-orders")

app = Flask(__name__)


@app.route("/api/health", methods=["GET"])
def health():
    """Health endpoint for checks / debugging (e.g. curl from nginx)."""
    return jsonify(status="ok", service="flask-orders")


@app.route("/api/orders", methods=["POST"])
def create_order():
    """Accept a new order. Full implementation arrives in steps 5 and 6:
    save to MySQL -> publish to Kafka -> notify via RabbitMQ.
    """
    logger.warning("POST /api/orders received, handler not implemented yet")
    return jsonify(status="not implemented yet"), 501


if __name__ == "__main__":
    logger.info("Starting Flask orders service on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000)
