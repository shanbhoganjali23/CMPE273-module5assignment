# Module 5 - Asynchronous Messaging (RabbitMQ)

For this lab I built a simple order system in Python with RabbitMQ. There are two programs:

- `producer.py` - sends order messages to a queue called `orders`
- `worker.py` - reads orders from the queue, processes them, and sends an ACK when done

The producer and the workers are separate processes, so I ran each one in its own terminal.

## Architecture

```
                        +------------------+
+------------+  publish |     RabbitMQ     |  deliver  +-----------+
|  Producer  | -------> |  queue: orders   | --------> | Worker 1  |
+------------+          |                  | --------> | Worker 2  |
                        |                  | <-- ACK --+-----------+
                        +------------------+                 |
                                                             v
                                                  processed_events.txt
```

Each message looks like this:

```json
{"event_id": "evt-98030fc3", "order_id": "ORD-0001", "item_id": "ITEM-467", "quantity": 2}
```

A few notes on how it works:

- Both workers listen on the same queue, so RabbitMQ gives each message to only one of them.
- I set `prefetch_count=1` so a worker only gets a new message after it ACKs the current one. Without this, one worker could grab a bunch of messages at once.
- `auto_ack` is off. The worker calls `basic_ack` only after the work is finished, so if a worker dies mid-message, RabbitMQ still has the message and gives it to the other worker.
- For duplicates, I save every processed `event_id` to `processed_events.txt`. I used a file instead of a set in memory so both workers can see the same list. There is a small file lock so the two workers don't write to it at the same time.

## How to run

1. Start RabbitMQ with Docker:

```
docker compose up -d
```

(or `docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management`)

The management page is at http://localhost:15672 (guest/guest).

2. Install pika:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Start two workers in two terminals:

```
python worker.py --name worker-1
python worker.py --name worker-2
```

4. Send some orders from a third terminal:

```
python producer.py --count 6
```

Other options I added for testing:

- `python worker.py --name worker-1 --crash-on ORD-0001` makes the worker quit before ACKing ORD-0001
- `python worker.py --name worker-1 --crash-on ORD-0001 --crash-after-process` makes it quit after processing but before the ACK
- `python producer.py --count 3 --duplicate` sends the first order a second time with the same event_id

Between tests I reset things with:

```
rm -f processed_events.txt processed_events.txt.lock
docker exec rabbitmq rabbitmqctl purge_queue orders
```

## Results

The full logs are in the `evidence/` folder. Each worker sleeps 1 second per order to pretend it's doing work.

### Successful processing

Producer output:

```
21:32:35 [producer] Published {"event_id": "evt-98030fc3", "order_id": "ORD-0001", "item_id": "ITEM-467", "quantity": 2}
21:32:35 [producer] Published {"event_id": "evt-a783c277", "order_id": "ORD-0002", "item_id": "ITEM-597", "quantity": 4}
21:32:35 [producer] Published {"event_id": "evt-8e760cfc", "order_id": "ORD-0003", "item_id": "ITEM-952", "quantity": 4}
21:32:35 [producer] Published {"event_id": "evt-b515ba56", "order_id": "ORD-0004", "item_id": "ITEM-786", "quantity": 3}
21:32:35 [producer] Published {"event_id": "evt-3de1a1a6", "order_id": "ORD-0005", "item_id": "ITEM-373", "quantity": 4}
21:32:35 [producer] Published {"event_id": "evt-ad3fa32e", "order_id": "ORD-0006", "item_id": "ITEM-770", "quantity": 2}
21:32:35 [producer] Done. Published 6 message(s).
```

One order going through the worker:

```
21:32:35 [worker-1] RECEIVED  ORD-0001 event_id=evt-98030fc3 redelivered=False delivery_tag=1
21:32:35 [worker-1] PROCESSING ORD-0001: item=ITEM-467 qty=2
21:32:36 [worker-1] ACKED     ORD-0001 event_id=evt-98030fc3
```

### Two competing consumers

In the same run, the 6 orders were split between the two workers and none of them were handled twice. Worker 1 got orders 1, 4, 6 and worker 2 got 2, 3, 5.

Worker 1:

```
21:32:35 [worker-1] RECEIVED  ORD-0001 event_id=evt-98030fc3 redelivered=False delivery_tag=1
21:32:36 [worker-1] ACKED     ORD-0001 event_id=evt-98030fc3
21:32:36 [worker-1] RECEIVED  ORD-0004 event_id=evt-b515ba56 redelivered=False delivery_tag=2
21:32:37 [worker-1] ACKED     ORD-0004 event_id=evt-b515ba56
21:32:37 [worker-1] RECEIVED  ORD-0006 event_id=evt-ad3fa32e redelivered=False delivery_tag=3
21:32:38 [worker-1] ACKED     ORD-0006 event_id=evt-ad3fa32e
```

Worker 2:

```
21:32:35 [worker-2] RECEIVED  ORD-0002 event_id=evt-a783c277 redelivered=False delivery_tag=1
21:32:36 [worker-2] ACKED     ORD-0002 event_id=evt-a783c277
21:32:36 [worker-2] RECEIVED  ORD-0003 event_id=evt-8e760cfc redelivered=False delivery_tag=2
21:32:37 [worker-2] ACKED     ORD-0003 event_id=evt-8e760cfc
21:32:37 [worker-2] RECEIVED  ORD-0005 event_id=evt-3de1a1a6 redelivered=False delivery_tag=3
21:32:38 [worker-2] ACKED     ORD-0005 event_id=evt-3de1a1a6
```

(I removed the PROCESSING lines here to keep it short; they're in the log files.)

Afterwards `rabbitmqctl list_queues` showed `orders 0 0`, so nothing was left in the queue.

### Failure and redelivery

I started worker 1 with `--crash-on ORD-0001`. It got ORD-0001 and then exited without sending the ACK:

```
21:32:45 [worker-1] RECEIVED  ORD-0001 event_id=evt-0c9eae2e redelivered=False delivery_tag=1
21:32:45 [worker-1] CRASH     simulating failure on ORD-0001 BEFORE processing/ACK (exiting)
```

Worker 2 then got ORD-0001 again. You can see `redelivered=True` on that line. It finished it along with the other orders:

```
21:32:45 [worker-2] RECEIVED  ORD-0002 event_id=evt-2ea4c6fc redelivered=False delivery_tag=1
21:32:46 [worker-2] ACKED     ORD-0002 event_id=evt-2ea4c6fc
21:32:46 [worker-2] RECEIVED  ORD-0001 event_id=evt-0c9eae2e redelivered=True delivery_tag=2
21:32:46 [worker-2] PROCESSING ORD-0001: item=ITEM-143 qty=2
21:32:47 [worker-2] ACKED     ORD-0001 event_id=evt-0c9eae2e
21:32:47 [worker-2] RECEIVED  ORD-0003 event_id=evt-8c4dfa2a redelivered=False delivery_tag=3
21:32:48 [worker-2] ACKED     ORD-0003 event_id=evt-8c4dfa2a
21:32:48 [worker-2] RECEIVED  ORD-0004 event_id=evt-be9d8a2f redelivered=False delivery_tag=4
21:32:49 [worker-2] ACKED     ORD-0004 event_id=evt-be9d8a2f
```

RabbitMQ and worker 2 kept running the whole time, and the queue was empty at the end. Worker 1 can be started again and it just rejoins.

### Duplicate handling

First I had the producer send ORD-0001 twice with the same event_id (`--duplicate`):

```
21:32:55 [producer] Published {"event_id": "evt-00c9a87a", "order_id": "ORD-0001", "item_id": "ITEM-979", "quantity": 5}
...
21:32:55 [producer] Re-publishing event_id=evt-00c9a87a to simulate a duplicate delivery
21:32:55 [producer] Published {"event_id": "evt-00c9a87a", "order_id": "ORD-0001", "item_id": "ITEM-979", "quantity": 5}
```

Worker 1 processed the first one:

```
21:32:55 [worker-1] RECEIVED  ORD-0001 event_id=evt-00c9a87a redelivered=False delivery_tag=1
21:32:55 [worker-1] PROCESSING ORD-0001: item=ITEM-979 qty=5
21:32:56 [worker-1] ACKED     ORD-0001 event_id=evt-00c9a87a
```

Worker 2 got the copy and skipped it:

```
21:32:56 [worker-2] RECEIVED  ORD-0001 event_id=evt-00c9a87a redelivered=False delivery_tag=2
21:32:56 [worker-2] DUPLICATE event_id=evt-00c9a87a (ORD-0001) already processed -> skipping business action, ACKing
```

I also tested a crash after processing but before the ACK (`--crash-after-process`). This is the case where a real duplicate comes from RabbitMQ itself:

```
21:33:05 [worker-1] RECEIVED  ORD-0001 event_id=evt-a1288cae redelivered=False delivery_tag=1
21:33:05 [worker-1] PROCESSING ORD-0001: item=ITEM-821 qty=5
21:33:06 [worker-1] CRASH     simulating failure on ORD-0001 AFTER processing, BEFORE ACK (exiting)
```

```
21:33:06 [worker-2] RECEIVED  ORD-0001 event_id=evt-a1288cae redelivered=True delivery_tag=2
21:33:06 [worker-2] DUPLICATE event_id=evt-a1288cae (ORD-0001) already processed -> skipping business action, ACKing
```

Without the event_id check, worker 2 would have processed ORD-0001 a second time.

## Reflection

A message can get delivered more than once because RabbitMQ only deletes it after it gets an ACK. If the worker crashes or loses its connection after doing the work but before the ACK goes through, RabbitMQ has no way to know the work was done, so it sends the message again. The producer can also send the same message twice if it retries. Idempotent handling fixes this: the worker checks the event_id first, and if it has already been processed, it skips the work and just ACKs. My last test shows this, where the order was processed, the worker crashed before the ACK, and the redelivered copy was ignored.

## Files

- `common.py` - connection settings and logging
- `producer.py` - order producer
- `worker.py` - order worker
- `docker-compose.yml` - runs RabbitMQ
- `evidence/` - log files from my test runs
