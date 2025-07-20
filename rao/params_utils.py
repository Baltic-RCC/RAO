import json
from pathlib import Path
from copy import deepcopy
from tempfile import NamedTemporaryFile
from loguru import logger

class ParameterOverride:
    """
    Context manager to override a nested key in a JSON config file, write it to a temp file and restore automatically after use.
    """
    def __init__(self, original_path: Path, keys_path: list[str], new_value):
        self.original_path = original_path
        self.keys_path = keys_path
        self.new_value = new_value
        self.temp_params = None

    def __enter__(self):
        """
        Reads the original config file, applies overrides that are defined, writes the modified config to a temp file and returns its path
        """
        original = json.loads(self.original_path.read_text())
        modified = deepcopy(original)

        current = modified
        for key in self.keys_path[:-1]:
            current = current.get(key, {})
        current[self.keys_path[-1]] = self.new_value

        self.temp_file = NamedTemporaryFile("w", delete=False, suffix=".json", prefix="params_", dir=self.original_path.parent)
        json.dump(modified, self.temp_file, indent=4)
        self.temp_file.close()
        logger.info(f"Using temporary altered parameters file for ID time horizon: {self.temp_file.name}")
        return Path(self.temp_file.name)

    def __exit__(self, *exc):
        """
        Deletes the temp file created in __enter__
        """
        if self.temp_file:
            Path(self.temp_file.name).unlink(missing_ok=True)
            logger.info(f"Temporary parameter file deleted: {self.temp_file.name}")
