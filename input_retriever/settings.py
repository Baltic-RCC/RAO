from config.settings import AppSettings, ConsumerSettings


class InputRetrieverConsumerSettings(ConsumerSettings):
    rmq_queue_in: str = "csa.object-storage.input"
    enable_data_storage_handler: bool = False


class InputRetrieverHandlerSettings(AppSettings):
    converter_key_mode: str = "local"
    elastic_contingencies_index: str = "csa-contingencies"
    elastic_assessed_elements_index: str = "csa-assessed-elements"
    elastic_remedial_actions_index: str = "csa-remedial-actions"
