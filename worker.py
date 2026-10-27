# Order worker - reads orders from the queue and ACKs after processing.
# Usage: python worker.py --name worker-1
import argparse
import json
import os
import time
from contextlib import contextmanager

from common import QUEUE_NAME, connect, declare_queue, get_logger

PROCESSED_FILE = os.getenv("PROCESSED_FILE", "processed_events.txt")
LOCK_FILE = PROCESSED_FILE + ".lock"


@contextmanager
def file_lock():
    # lock so both workers don't use the file at the same time
    # (fcntl on Mac/Linux, msvcrt on Windows)
    with open(LOCK_FILE, "a+") as lock:
        if os.name == "nt":
            import msvcrt
            lock.seek(0)
            while True:
                try:
                    msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    time.sleep(0.05)
            try:
                yield
            finally:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)


def load_processed() -> set[str]:
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE) as f:
        return {line.strip() for line in f if line.strip()}


def is_processed(event_id: str) -> bool:
    with file_lock():
        return event_id in load_processed()


def mark_processed(event_id: str) -> None:
    with file_lock():
        with open(PROCESSED_FILE, "a") as f:
            f.write(event_id + "\n")
            f.flush()
            os.fsync(f.fileno())


def do_business_action(order: dict, seconds: float) -> None:
    # fake work (would be something like reserving stock)
    time.sleep(seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume order messages")
    parser.add_argument("--name", default=f"worker-{os.getpid()}")
    parser.add_argument("--work-seconds", type=float, default=1.0,
                        help="simulated processing time per message")
    parser.add_argument("--crash-on", metavar="ORDER_ID",
                        help="simulate a crash (exit before ACK) when this order_id is received")
    parser.add_argument("--crash-after-process", action="store_true",
                        help="with --crash-on: crash AFTER the business action but BEFORE the ACK")
    args = parser.parse_args()

    log = get_logger(args.name)
    conn = connect()
    channel = conn.channel()
    declare_queue(channel)
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        order = json.loads(body)
        event_id = order["event_id"]
        log.info("RECEIVED  %s event_id=%s redelivered=%s delivery_tag=%s",
                 order["order_id"], event_id, method.redelivered, method.delivery_tag)

        if is_processed(event_id):
            log.info("DUPLICATE event_id=%s (%s) already processed -> skipping business action, ACKing",
                     event_id, order["order_id"])
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        crash_here = args.crash_on and order["order_id"] == args.crash_on

        if crash_here and not args.crash_after_process:
            log.info("CRASH     simulating failure on %s BEFORE processing/ACK (exiting)", order["order_id"])
            os._exit(1)  # quit right away, no ACK

        log.info("PROCESSING %s: item=%s qty=%s", order["order_id"], order["item_id"], order["quantity"])
        do_business_action(order, args.work_seconds)
        mark_processed(event_id)

        if crash_here and args.crash_after_process:
            log.info("CRASH     simulating failure on %s AFTER processing, BEFORE ACK (exiting)", order["order_id"])
            os._exit(1)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("ACKED     %s event_id=%s", order["order_id"], event_id)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    log.info("Waiting for messages on queue '%s' (CTRL+C to stop)", QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("Shutting down")
        channel.stop_consuming()
        conn.close()


if __name__ == "__main__":
    main()
