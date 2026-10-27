# Order producer - sends order messages to the orders queue.
# Usage: python producer.py --count 6
import argparse
import json
import random
import uuid

import pika

from common import QUEUE_NAME, connect, declare_queue, get_logger

log = get_logger("producer")


def make_order(order_num: int, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or f"evt-{uuid.uuid4().hex[:8]}",
        "order_id": f"ORD-{order_num:04d}",
        "item_id": f"ITEM-{random.randint(100, 999)}",
        "quantity": random.randint(1, 5),
    }


def publish(channel, order: dict) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=order["event_id"],
        ),
    )
    log.info("Published %s", json.dumps(order))


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=5, help="number of orders to publish")
    parser.add_argument("--start", type=int, default=1, help="first order number")
    parser.add_argument("--event-id", help="use this exact event_id (only with --count 1)")
    parser.add_argument("--duplicate", action="store_true",
                        help="after publishing, re-publish the first message with the SAME event_id")
    args = parser.parse_args()

    conn = connect()
    channel = conn.channel()
    declare_queue(channel)

    sent = []
    for i in range(args.count):
        order = make_order(args.start + i, args.event_id if args.count == 1 else None)
        publish(channel, order)
        sent.append(order)

    if args.duplicate and sent:
        log.info("Re-publishing event_id=%s to simulate a duplicate delivery", sent[0]["event_id"])
        publish(channel, sent[0])

    conn.close()
    log.info("Done. Published %d message(s).", len(sent) + (1 if args.duplicate and sent else 0))


if __name__ == "__main__":
    main()
