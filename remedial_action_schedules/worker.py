import config
from integrations import rmq
from remedial_action_schedules.handlers import HandlerRemedialActionScheduleToElastic
from common.config_parser import parse_app_properties
from loguru import logger
from pathlib import Path
from uuid import uuid4

parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="CONSUMER",
                     eval_types=True)

# Set worker name and unique id to Elastic log handler
worker_id = str(uuid4())
if getattr(config.initialize_logging, 'elastic_handler', None):
    config.initialize_logging.elastic_handler.extra.update({'worker': 'remedial-action-schedules', 'worker_id': worker_id})

# RabbitMQ consumer implementation
logger.info(f"Starting 'remedial-action-schedules' worker with assigned trace id: {worker_id}")
consumer = rmq.RMQConsumer(
    queue=RMQ_QUEUE_IN,
    message_handlers=[HandlerRemedialActionScheduleToElastic()],
)
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
