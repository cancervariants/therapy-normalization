"""ETL methods for NCIt source"""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.models import Meta
from therapy.schemas import SourceName
from sqlalchemy.orm import Session


class NCIt(Base):
    """Core NCIt ETL class"""

    def _extract_data(self, *args, **kwargs):
        """Get NCIt source file"""
        if 'data_path' in kwargs:
            self._data_src = kwargs['data_path']
        else:
            data_dir = PROJECT_ROOT / 'data' / 'ncit'
            data_dir.mkdir(exist_ok=True, parents=True)
            try:
                self._data_src = sorted(list(data_dir.iterdir()))[-1]
            except IndexError:
                raise FileNotFoundError  # TODO download function here
        self._version = self._data_src.stem.split('_')[1]

    def _transform_data(self, *args, **kwargs):
        raise NotImplementedError

    def _add_meta(self, db: Session):
        meta_object = Meta(src_name=SourceName.NCIT.value,
                           data_license="CC BY 4.0",
                           data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa F401
                           version=self._version,
                           data_url="https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/Thesaurus_20.09d.OWL.zip",)  # noqa F401
        db.add(meta_object)
