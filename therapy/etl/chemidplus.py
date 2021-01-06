"""ETL methods for ChemIDPlus source."""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.database import Database
from pathlib import Path


class ChemIDplus(Base):
    """Core ChemIDplus ETL class."""

    def __init__(self,
                 database: Database,
                 # TODO figure out data repo info
                 src_dir: str = 'ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/',  # noqa: E501
                 src_fname: str = None,
                 data_path: Path = PROJECT_ROOT / 'data' / 'chemidplus'):
        """Initialize class instance.

        :param therapy.database.Database database: database instance to use
        :param str src_dir: URL to directory containing source file
        :param str src_fname: name of file as stored in src_dir
        """
        self.database = database
        self.src_dir = src_dir
        self.src_fname = src_fname
        self._extract_data(data_path)
        self._transform_data()

    def _download_data(self):
        raise NotImplementedError

    def _extract_data(self, data_dir):
        data_dir.mkdir(exist_ok=True, parents=True)
        dir_files = list(data_dir.iterdir())
        if len(dir_files) == 0:
            self._download_data()
            dir_files = list(data_dir.iterdir())
        self._data_src = sorted(dir_files)[-1]
        self._version = self._data_src.stem.split('_')[1]
