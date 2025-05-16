import json
from uuid import uuid4
from loguru import logger
import config
from rao.integrations import rmq
from rao.input_retriever.handlers import HandlerMetadataToObjectStorage
from rao.common.config_parser import parse_app_properties

parse_app_properties(caller_globals=globals(), path=config.paths.input_retriever.input_retriever)

# RabbitMQ consumer implementation
consumer = rmq.RMQConsumer(
    queue=RMQ_QUEUE_IN,
    message_handlers=[HandlerMetadataToObjectStorage()],
)
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
