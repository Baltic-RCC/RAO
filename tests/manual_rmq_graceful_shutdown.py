"""
Manual test: graceful shutdown of RMQConsumer while a message is being processed.

Publishes one test message to a dedicated test queue, consumes it with a handler that
sleeps --processing-time seconds and expects SIGTERM/SIGINT to arrive during processing.
After the consumer exits it verifies that the message was ACKed (not requeued).

Use --processing-time > 30 to also prove heartbeats keep flowing while shutdown waits
(broker drops a connection after ~2x heartbeat = 30s of silence).

Usage (RMQ connection taken from the same env/settings as workers):
  # send the signal yourself: CTRL+C, or from another shell `kill -TERM <pid>`
  python tests/manual_rmq_graceful_shutdown.py --processing-time 60

  # send SIGTERM automatically N seconds after processing started
  python tests/manual_rmq_graceful_shutdown.py --processing-time 60 --signal-after 5

  # inside a Kubernetes pod
  kubectl cp tests/manual_rmq_graceful_shutdown.py <ns>/<pod>:/tmp/manual_rmq_graceful_shutdown.py
  kubectl exec -it <pod> -n <ns> -- python /tmp/manual_rmq_graceful_shutdown.py --processing-time 60
  kubectl exec <pod> -n <ns> -- kill -TERM <pid printed by the script>
"""
import argparse
import os
import signal
import sys
import threading
import time
import pika
from loguru import logger
from integrations import rmq


class SleepHandler:
    """Simulates long message processing"""

    def __init__(self, seconds: int, on_start=None):
        self.seconds = seconds
        self.on_start = on_start
        self.finished = False

    def handle(self, message: bytes, properties: pika.BasicProperties, **kwargs):
        logger.info(f"SleepHandler: processing for {self.seconds}s -> send SIGTERM now (pid {os.getpid()})")
        if self.on_start:
            self.on_start()
        for i in range(self.seconds):
            if i % 10 == 0:
                logger.info(f"SleepHandler: still processing ({i}/{self.seconds}s)")
            time.sleep(1)
        self.finished = True
        logger.info("SleepHandler: processing finished")
        return message, properties


def blocking_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=rmq.conf.rmq_server,
        port=rmq.conf.rmq_port,
        virtual_host=rmq.conf.rmq_vhost,
        credentials=pika.PlainCredentials(rmq.conf.rmq_username, rmq.conf.rmq_password.get_secret_value()),
    ))


def send_signal(signum):
    logger.info(f"Sending {signal.Signals(signum).name} to self")
    if os.name == "nt":
        signal.raise_signal(signum)  # os.kill on Windows terminates the process without running handlers
    else:
        os.kill(os.getpid(), signum)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--queue", default="rao-graceful-shutdown-test")
    parser.add_argument("--processing-time", type=int, default=60, help="Seconds the handler sleeps")
    parser.add_argument("--signal-after", type=int, default=None,
                        help="Automatically send SIGTERM N seconds after processing started")
    parser.add_argument("--keep-queue", action="store_true", help="Do not delete the test queue at the end")
    args = parser.parse_args()

    logger.info(f"Test process pid: {os.getpid()}")

    # Prepare test queue with exactly one message
    connection = blocking_connection()
    channel = connection.channel()
    channel.queue_declare(queue=args.queue, durable=True, auto_delete=False)
    channel.queue_purge(queue=args.queue)
    channel.basic_publish(exchange="", routing_key=args.queue, body=b"graceful-shutdown-test",
                          properties=pika.BasicProperties(app_id="graceful-shutdown-test", headers={"test": True}))
    connection.close()
    logger.info(f"Published test message to queue '{args.queue}'")

    on_start = None
    if args.signal_after is not None:
        on_start = lambda: threading.Timer(args.signal_after, send_signal, args=(signal.SIGTERM,)).start()
    handler = SleepHandler(args.processing_time, on_start=on_start)

    consumer = rmq.RMQConsumer(queue=args.queue, message_handlers=[handler])
    started = time.time()
    consumer.run()
    elapsed = time.time() - started

    # Verify outcome: unacked message would be back in 'ready' state after consumer connection closed
    connection = blocking_connection()
    channel = connection.channel()
    message_count = channel.queue_declare(queue=args.queue, passive=True).method.message_count
    if not args.keep_queue:
        channel.queue_delete(queue=args.queue)
    connection.close()

    checks = {
        "signal received": consumer._shutdown_requested,
        "handler finished processing": handler.finished,
        "message ACKed (queue empty)": message_count == 0,
    }
    logger.info(f"Consumer exited after {elapsed:.1f}s")
    for name, ok in checks.items():
        logger.info(f"[{'PASS' if ok else 'FAIL'}] {name}")

    if all(checks.values()):
        logger.info("RESULT: PASS - message finished and ACKed before shutdown")
        return 0
    logger.error(f"RESULT: FAIL - messages left in queue: {message_count}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
