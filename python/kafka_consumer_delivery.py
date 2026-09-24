"""Kafka consumer #2 (step 10): the "delivery" side of the order pipeline.

Reads the same `order-events` topic in its own consumer group
`delivery-group`, so it receives every message independently of any other
group (e.g. `payment-group`) — group offsets are kept per group by the
broker. For each order it logs that the delivery has been prepared.
"""
import json
import logging
import os
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

import env

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
logger = logging.getLogger("kafka-delivery")

# Own group name: the payment consumer uses `payment-group`, so both groups
# see the full topic (Kafka delivers to every group, offsets are per-group).
KAFKA_GROUP = os.environ.get("KAFKA_GROUP", "delivery-group")


def _make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        env.KAFKA_TOPIC,
        bootstrap_servers=env.kafka_bootstrap(),
        group_id=KAFKA_GROUP,
        # On the first join of a fresh lab group, replay already-published
        # orders too, so `kafka-consumer-groups.sh --describe` shows the
        # group with lag 0 after it catches up.
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8"),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def main() -> None:
    logger.info(
        "Kafka delivery consumer starting -> group=%s topic=%s broker=%s",
        KAFKA_GROUP,
        env.KAFKA_TOPIC,
        env.kafka_bootstrap(),
    )
    while True:
        consumer = None
        try:
            consumer = _make_consumer()
            for msg in consumer:
                event = msg.value or {}
                logger.info(
                    "доставка по order_id=%s подготовлена "
                    "(buyer=%s product=%s amount=%s partition=%s)",
                    event.get("order_id"),
                    event.get("buyer"),
                    event.get("product"),
                    event.get("amount"),
                    msg.partition,
                )
        except KafkaError as exc:
            # Broker hiccup: log and re-create the consumer. Offsets are
            # committed to the group, so no messages are lost.
            logger.warning("Kafka consumer error (%s), reconnecting in 5s", exc)
            time.sleep(5)
        except Exception:
            logger.exception("unexpected error in delivery consumer, restarting in 5s")
            time.sleep(5)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
