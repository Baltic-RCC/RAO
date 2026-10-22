import config
from integrations import rmq
from rao.handlers import HandlerOptimizer
from rao.settings import OptimizerConsumerSettings
from loguru import logger
from uuid import uuid4
import sys

conf = OptimizerConsumerSettings()

# Set worker name and unique id to Elastic log handler
worker_id = str(uuid4())
if getattr(config.initialize_logging, 'elastic_handler', None):
    config.initialize_logging.elastic_handler.extra.update({'worker': 'rao-optimizer', 'worker_id': worker_id})

logger.info(f"Starting 'rao-optimizer' worker with assigned trace id: {worker_id}")

if conf.consumer_type == "SINGLE_MESSAGE":
    # RabbitMQ single message consumer implementation aligned with KEDA usage
    consumer = rmq.SingleMessageConsumer(
        queue=conf.rmq_queue_in,
        message_handlers=[HandlerOptimizer()],
    )
    sys.exit(consumer.run())
elif conf.consumer_type == "LONG_LIVING":
    # RabbitMQ long-living consumer implementation
    consumer = rmq.RMQConsumer(
        queue=conf.rmq_queue_in,
        message_handlers=[HandlerOptimizer()],
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()
else:
    raise Exception(f"Unknown CONSUMER_TYPE: {conf.consumer_type}")