import config
from integrations import rmq
from rao.optimizer_handler import HandlerOptimizer
from common.config_parser import parse_app_properties
from loguru import logger
from pathlib import Path
from uuid import uuid4
import sys

parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="OPTIMIZER_CONSUMER",
                     eval_types=True)

# Set worker name and unique id to Elastic log handler
worker_id = str(uuid4())
if getattr(config.initialize_logging, 'elastic_handler', None):
    config.initialize_logging.elastic_handler.extra.update({'worker': 'optimizer-runner', 'worker_id': worker_id})

logger.info(f"Starting 'optimizer-runner' worker with assigned trace id: {worker_id}")

if CONSUMER_TYPE == "SINGLE_MESSAGE":
    # RabbitMQ single message consumer implementation aligned with KEDA usage
    consumer = rmq.SingleMessageConsumer(
        queue=RMQ_QUEUE_IN,
        message_handlers=[HandlerOptimizer()],
    )
    sys.exit(consumer.run())
elif CONSUMER_TYPE == "LONG_LIVING":
    # RabbitMQ long-living consumer implementation
    consumer = rmq.RMQConsumer(
        queue=RMQ_QUEUE_IN,
        message_handlers=[HandlerOptimizer()],
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()
else:
    raise Exception("Unknown CONSUMER_TYPE, please check the config.properties file")
