"""The VICC library for normalizing therapies."""
from pathlib import Path
import logging

PROJECT_ROOT = Path(__file__).resolve().parents[1]
logging.basicConfig(
    filename='therapy.log',
    format='[%(asctime)s] %(levelname)s : %(message)s')
logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)
