from integrations import rmq
from input_retriever.handlers import HandlerMetadataToObjectStorage
from common.config_parser import parse_app_properties
from loguru import logger
from pathlib import Path

parse_app_properties(caller_globals=globals(), path=str(Path(__file__).parent.joinpath("config.properties")))

# RabbitMQ consumer implementation
consumer = rmq.RMQConsumer(
    queue=RMQ_QUEUE_IN,
    message_handlers=[HandlerMetadataToObjectStorage()],
)
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
