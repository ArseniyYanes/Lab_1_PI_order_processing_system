"""Kafka consumer #1 — payment processing (step 9).

Runs in its own consumer group `payment-group` and reads the `order-events`
topic. For every order event it simulates the payment step and logs the
result ("payment processed for order #...").

Kafka keeps one copy of each message per consumer group, so this consumer
sees the full stream: the `delivery-group` consumer (step 10) reads the same
events independently, meaning both "services" receive every order.

Run inside the container:  python payment_consumer.py
"""
import json
import logging
import time

from kafka import KafkaConsumer

import env

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
logger = logging.getLogger("payment-consumer")

GROUP_ID = "payment-group"


def _decode_key(k):
    """The producer stores the order_id as the message key (str -> utf-8)."""
    if k is None:
        return None
    try:
        return int(k.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return k.decode("utf-8", "replace")


def _decode_value(v):
    """Values are JSON dicts produced by app.py; fall back to the raw text."""
    if v is None:
        return {}
    raw = v.decode("utf-8", "replace")
    try:
        return json.loads(raw)
    except ValueError:
        return {"raw": raw}


def main():
    logger.info(
        "Payment consumer starting -> %s (topic=%s, group=%s)",
        env.kafka_bootstrap(), env.KAFKA_TOPIC, GROUP_ID,
    )
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                env.KAFKA_TOPIC,
                bootstrap_servers=env.kafka_bootstrap(),
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                key_deserializer=_decode_key,
                value_deserializer=_decode_value,
            )
            logger.info("connected, consuming %s in group %s", env.KAFKA_TOPIC, GROUP_ID)
            for msg in consumer:
                data = msg.value or {}
                order_id = data.get("order_id", msg.key)
                logger.info(
                    "payment processed for order #%s (buyer=%s product=%s amount=%s)",
                    order_id, data.get("buyer"), data.get("product"), data.get("amount"),
                )
        except KeyboardInterrupt:
            logger.info("payment consumer stopped by user")
            break
        except Exception as exc:
            logger.warning("kafka consumer error: %s (reconnecting in 3s)", exc)
            time.sleep(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
