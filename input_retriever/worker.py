import config
from integrations import rmq
from input_retriever.handlers import HandlerMetadataToObjectStorage, HandlerInputDataToElastic
from input_retriever.settings import InputRetrieverConsumerSettings
from loguru import logger
from uuid import uuid4
import signal


consumer_settings = InputRetrieverConsumerSettings()


# Set worker name and unique id to Elastic log handler
worker_id = str(uuid4())
worker_name = "input-retriever"
if getattr(config.initialize_logging, 'elastic_handler', None):
    config.initialize_logging.elastic_handler.extra.update({'worker': worker_name, 'worker_id': worker_id})

# RabbitMQ consumer implementation
logger.info(f"Starting {worker_name} worker with assigned trace id: {worker_id}")
handlers = [HandlerMetadataToObjectStorage()]
if consumer_settings.enable_data_storage_handler:
    handlers.append(HandlerInputDataToElastic())
consumer = rmq.RMQConsumer(
    queue=consumer_settings.rmq_queue_in,
    message_handlers=handlers,
)


def handle_shutdown(signum, frame):
    """
    Handle SIGTERM / SIGINT from Kubernetes (or the OS).

    We delegate to consumer.request_shutdown() which:
      - sets a flag so no new reconnects are attempted
      - lets the in-flight message finish processing and be ACKed
      - then stops the ioloop and drains the thread pool cleanly

    We do NOT call consumer.stop() directly here because stop() calls
    pika channel/ioloop methods and those must be invoked from the ioloop
    thread, not from an async signal handler.
    """
    logger.info(f"Received signal {signal.Signals(signum).name}, requesting graceful shutdown")
    consumer.request_shutdown()


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
