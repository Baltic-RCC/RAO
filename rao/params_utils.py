import json
from ast import Bytes
from pathlib import Path
from copy import deepcopy
from io import BytesIO
from textwrap import indent
from typing import Union
from loguru import logger

class ParameterOverride:
    """
    Context manager to override a nested key in a JSON config file, write it to a BytesIO object and close the BytesIO stream after use.
    """
    def __init__(self, original_path: Path, keys_path: list[str], new_value: Union[str, int, float, bool]):
        self.original_path = original_path
        self.keys_path = keys_path
        self.new_value = new_value
        self.temp_buffer = None

    def __enter__(self) -> BytesIO:
        """
        Reads the original config file, applies overrides that are defined, writes the modified config to a BytesIO object
        """
        original = json.loads(self.original_path.read_text())
        modified = deepcopy(original)

        # Navigate through nested JSON parameter file and override modifications
        current = modified
        for key in self.keys_path[:-1]:
            current = current.get(key, {})
        current[self.keys_path[-1]] = self.new_value

        # Convert to BytesIO object
        json_bytes = json.dumps(modified, indent=4).encode("utf-8")
        self.temp_buffer = BytesIO(json_bytes)
        self.temp_buffer.name = "parameters_override.json"

        logger.info(f"In-memory config created with parameter overrides: {'.'.join(self.keys_path)} = {self.new_value}")
        return self.temp_buffer

    def __exit__(self, *exc):
        if self.temp_buffer:
            self.temp_buffer.close()
            logger.info(f"BytesIO stream closed")
