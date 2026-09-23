"""RabbitMQ worker (step 8): consumes the `order-notifications` queue.

The Flask publisher (app.py) puts one short text message per new order
into the durable queue `order-notifications` with delivery_mode=2, so a
message survives a broker restart and is not lost if this worker is
down. Here the worker:

  1. re-declares the same queue with the same parameters (durable=True),
     so it is safe to start before or after the publisher;
  2. pulls messages one by one (basic_qos prefetch=1) and "delivers"
     them to the recipient -- for the lab the delivery is simulated by
     logging the notification to the console;
  3. sends basic_ack only after delivery, so an unacknowledged message
     is redelivered if the worker crashes or the connection drops;
  4. reconnects automatically when the broker disappears.

Run inside the container:  python rabbit_worker.py
"""
import logging
import time

import pika

import env

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
logger = logging.getLogger("rabbit-worker")

QUEUE = env.RABBITMQ_QUEUE


def on_message(channel, _method, properties, body):
    """Deliver one notification, then acknowledge it.

    pika passes the BlockingChannel itself as the first callback argument.
    """
    message = body.decode("utf-8")
    # delivery_mode=2 from the publisher -> persistent message
    logger.info(
        "notification received (persistent=%s): %s", bool(properties.delivery_mode == 2), message
    )
    # Simulate delivering the notification (e.g. sending an e-mail/SMS).
    time.sleep(0.5)
    logger.info("notification delivered to customer: %s", message)
    channel.basic_ack(delivery_tag=_method.delivery_tag)


def main():
    logger.info(
        "RabbitMQ worker starting -> %s (queue=%s)", env.rabbitmq_url(), QUEUE
    )
    while True:
        connection = None
        try:
            connection = pika.BlockingConnection(pika.URLParameters(env.rabbitmq_url()))
            channel = connection.channel()
            # Same parameters as the publisher: durable queue.
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
            logger.info("worker connected, consuming from %s", QUEUE)
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("worker stopped by user")
            if connection and connection.is_open:
                channel = connection.channel()
                channel.stop_consuming()
                connection.close()
            return
        except Exception as exc:
            logger.warning("worker connection lost: %s", exc)
            time.sleep(3)
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
