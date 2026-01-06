import logging
import inspect
from loguru import logger
import sys
from pathlib import Path
import datetime
import traceback
import time
from elasticsearch import Elasticsearch
import pypowsybl
from common.config_parser import parse_app_properties


parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("logging/logging.properties")),
                     eval_types=True)


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = inspect.currentframe(), 0
        while frame:
            filename = frame.f_code.co_filename
            is_logging = filename == logging.__file__
            is_frozen = "importlib" in filename and "_bootstrap" in filename
            if depth > 0 and not (is_logging or is_frozen):
                break
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).bind(std_log=True).log(level, record.getMessage())


class ElasticLogHandler:

    def __init__(self, server: str, index: str, logs_rollover: bool = False, extra: dict | None = None):
        self.server = server
        self.index = index
        self.logs_rollover = logs_rollover
        self.extra = extra if extra else {}
        self.client = Elasticsearch(self.server)

        self._connected = True
        self._last_retry = 0
        self._retry_interval = 30

    def write(self, message):
        now = time.time()
        if not self._connected and now - self._last_retry < self._retry_interval:
            return

        record = message.record

        # Extract stack trace if available
        exception = record.get('exception')
        stack_trace = None
        if exception and any(list(exception)):
            stack_trace = "".join(traceback.format_exception(*exception))

        log_entry = {
            "@timestamp": record["time"].isoformat(),
            "levelname": record["level"].name,
            "levelno": record["level"].no,
            "message": record["message"],
            "file": record["file"].name,
            "module": record["module"],
            "name": record["name"],
            "function": record["function"],
            "line": record["line"],
            "process": record["process"].id,
            "thread": record["thread"].id,
            "exception": str(record["exception"]) if exception else None,
            "exception_trace": stack_trace,
            **record["extra"],  # add extra fields from loguru
            **self.extra,  # add extra fields from self
        }

        index = self.index
        if self.logs_rollover:
            index = f"{self.index}-{datetime.datetime.today():%Y%m}"

        try:
            self.client.index(index=index, document=log_entry)
            self._connected = True
        except Exception as e:
            self._last_retry = now
            self._connected = False
            print(f"Error while sending log message to Elasticsearch, disable sink: {e}", file=sys.stderr)


def std_log_filter(record):
    """Method to suspend logs from standard python loggers"""
    if record["extra"].get("std_log"):
        return FORWARD_STD_LOGS
    return True


# Define log format
# LOG_FORMAT = (
#     "<green>{time:YYYY-MM-DD HH:mm:ss:SSS}</green> | "
#     "<level>{level: <8}</level> | "
#     "<cyan>{name: <50}</cyan> | "
#     "<cyan>{function: <40}</cyan> | "
#     "<cyan>{line: <4}</cyan> | "
#     # "<cyan>{name}:{function}:{line:<50}</cyan> | "
#     "<level>{message}</level>"
# )

# Configure STDERR (stream) sink according configuration
logging.basicConfig(handlers=[InterceptHandler()], level=LOG_LEVEL, force=True)
logger.remove()  # remove all existing handlers to have fresh setup
logger.add(sink=sys.stderr, level=LOG_LEVEL, filter=std_log_filter)
logger.debug(f"Stream logger initialized with level: {LOG_LEVEL}")

# Configure Elastic sink according configuration
parse_app_properties(caller_globals=globals(), path=str(Path(__file__).parent.joinpath("integrations/elastic.properties")))
if ELASTIC_LOGS_HANDLER:
    # Disable loggers which generates messages during sink method to elasticsearch
    for logger_name in ["elastic_transport", "urllib3"]:
        es_logger = logging.getLogger(logger_name)
        es_logger.propagate = False
        es_logger.setLevel("WARNING")
    elastic_handler = ElasticLogHandler(server=ELK_SERVER, index=ELASTIC_LOGS_INDEX)
    logger.add(sink=elastic_handler.write,
               level=LOG_LEVEL,
               serialize=True,
               enqueue=ENQUEUE,
               catch=CATCH,
               filter=std_log_filter)

# Expose powsybl logs if defined by configuration
if ENABLE_POWSYBL_LOGS:
    logging.getLogger("powsybl").setLevel(LOG_LEVEL)
    logger.info(f"Powsybl logs enabled with level: {LOG_LEVEL}")

# Exclude logs from ingration modules
modules_to_exclude = EXCLUDE_LOGGERS.split(",") if EXCLUDE_LOGGERS else []
if modules_to_exclude:
    for m in modules_to_exclude:
        logging.getLogger(m).propagate = False