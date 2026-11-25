from config.settings import AppSettings, ConsumerSettings


class SchedulesConsumerSettings(ConsumerSettings):
    rmq_queue_in: str = "csa.schedules.ras"


class SchedulesHandlerSettings(AppSettings):
    converter_key_mode: str = "local"
    elastic_schedules_index: str = "csa-optimized-schedules"
