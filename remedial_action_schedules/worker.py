import config
from integrations import rmq
from remedial_action_schedules.handlers import HandlerRemedialActionScheduleToElastic
from remedial_action_schedules.settings import SchedulesConsumerSettings
from loguru import logger
from uuid import uuid4

consumer_settings = SchedulesConsumerSettings()

# Set worker name and unique id to Elastic log handler
worker_id = str(uuid4())
if getattr(config.initialize_logging, 'elastic_handler', None):
    config.initialize_logging.elastic_handler.extra.update({'worker': 'remedial-action-schedules', 'worker_id': worker_id})

# RabbitMQ consumer implementation
logger.info(f"Starting 'remedial-action-schedules' worker with assigned trace id: {worker_id}")
consumer = rmq.RMQConsumer(
    queue=consumer_settings.rmq_queue_in,
    message_handlers=[HandlerRemedialActionScheduleToElastic()],
)
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
