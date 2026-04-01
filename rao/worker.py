import config
from integrations import rmq
from rao.handlers import HandlerVirtualOperator
from common.config_parser import parse_app_properties
from loguru import logger
from pathlib import Path
from uuid import uuid4
import sys
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
from common.telemetry import init_tracing


parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="CONSUMER",
                     eval_types=True)

# Set worker name and unique id to Elastic log handler
worker_id = str(uuid4())
worker_name = "optimizer"
if getattr(config.initialize_logging, 'elastic_handler', None):
    config.initialize_logging.elastic_handler.extra.update({'worker': worker_name, 'worker_id': worker_id})

# Initiate resource and provider once
if ENABLE_OTEL:
    resource = Resource.create({"service.name": "rao"})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    init_tracing(provider)
    tracer = trace.get_tracer(__name__)
    logger.info("OpenTelemetry tracing initialized")

logger.info(f"Starting {worker_name} worker with assigned trace id: {worker_id}")

if CONSUMER_TYPE == "SINGLE_MESSAGE":
    # RabbitMQ single message consumer implementation aligned with KEDA usage
    consumer = rmq.SingleMessageConsumer(
        queue=RMQ_QUEUE_IN,
        message_handlers=[HandlerVirtualOperator()],
    )
    sys.exit(consumer.run())
elif CONSUMER_TYPE == "LONG_LIVING":
    # RabbitMQ long-living consumer implementation
    consumer = rmq.RMQConsumer(
        queue=RMQ_QUEUE_IN,
        message_handlers=[HandlerVirtualOperator()],
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()
else:
    raise Exception("Unknown CONSUMER_TYPE, please check the config.properties file")