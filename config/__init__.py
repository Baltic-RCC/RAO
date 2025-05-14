from pathlib import Path
import logging

Path.read = Path.read_text

logger = logging.getLogger(__name__)

# Get the directory path of the configuration files
config_directory = Path(__file__).resolve().parent


# Create empty classes to store data
class Paths():
    pass


class Attribute():

    pass

# List to store all configuration file paths
paths = Paths()
dirs_to_check = [config_directory]

# Recursively search for files in all folders
for path in dirs_to_check:
    for child_path in path.iterdir():

        # If path analyse further
        if child_path.is_dir():
            dirs_to_check.append(child_path)

        if "__" in child_path.stem:
            continue

        # Add the full path of the configuration file
        if child_path.is_file():
            logger.debug(f"Found config file {child_path.resolve()}")

            if not getattr(paths, child_path.parent.name, None):
                setattr(paths, child_path.parent.name, Attribute())

            setattr(getattr(paths, child_path.parent.name), child_path.stem, child_path.resolve())



