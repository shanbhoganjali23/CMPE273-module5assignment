# settings shared by producer.py and worker.py
import logging
import os
import sys

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "orders")


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("pika").setLevel(logging.WARNING)  # pika logs a lot otherwise
    return logging.getLogger(name)


def connect() -> pika.BlockingConnection:
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
        heartbeat=30,
    )
    return pika.BlockingConnection(params)


def declare_queue(channel) -> None:
    # durable so the queue survives a RabbitMQ restart
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
