"""The VICC library for normalizing therapies."""
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[1]
logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)
