import logging
import json
from uuid import uuid4

# Initialize custom logger
logger = logging.getLogger(__name__)

import config
from rao.integrations.elastic import HandlerSendToElastic
from rao.integrations import rmq


parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

# RabbitMQ consumer implementation
consumer = rmq.RMQConsumer(
    queue=INPUT_RMQ_QUEUE,
    message_handlers=[
        HandlerModelsToMinio(),
        HandlerSendToElastic(index=METADATA_ELK_INDEX,
                             id_from_metadata=True,
                             id_metadata_list=ELK_ID_FROM_METADATA_FIELDS.split(','),
                             hashing=json.loads(ELK_ID_HASHING.lower()),
                             ),
    ])
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
