from pathlib import Path
from . import initialize_logging

Path.read = Path.read_text
