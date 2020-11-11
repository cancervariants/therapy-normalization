"""ETL methods for NCIt source"""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.models import Meta, Therapy, OtherIdentifier, Alias
from therapy.schemas import SourceName, NamespacePrefix
from therapy.database import Base as B
from therapy.database import SessionLocal, engine
from sqlalchemy.orm import Session
import owlready2 as owl
from owlready2.entity import ThingClass
from typing import Set
import requests
import zipfile
import logging
from os import remove, rename

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class NCIt(Base):
    """Core NCIt ETL class.

    Extracting both:
     * NCIt classes with semantic_type "Pharmacologic Substance"
     * NCIt classes that are subclasses of C1909 (Pharmacologic Substance)
    """

    def __init__(self,
                 src_dirname="https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/",  # noqa F401
                 src_fname="Thesaurus_20.09d.OWL.zip",
                 *args,
                 **kwargs):
        """Override base class init method. Call ETL methods with generated
        db SessionLocal instance.
        """
        B.metadata.create_all(bind=engine)
        self._SRC_DIR = src_dirname
        self._SRC_FNAME = src_fname
        self._extract_data()
        db: Session = SessionLocal()
        self._transform_data(db)
        db.commit()
        db.close()

    def _load_data(self, therapy: Therapy):
        """Load data from individual NCIt entry into db"""

    def get_desc_nodes(self, node: ThingClass,
                       uq_nodes: Set[ThingClass]) -> Set[ThingClass]:
        """Create set of unique subclasses of node parameter.
        Originally called on C1909, Pharmacologic Substance.
        """
        children = node.descendants()
        if children:
            for child_node in children:
                if child_node is not node:
                    uq_nodes.add(child_node)
                    self.get_desc_nodes(child_node, uq_nodes)
        return uq_nodes

    def get_typed_nodes(self, uq_nodes: Set[ThingClass],
                        ncit: owl.namespace.Ontology) -> Set[ThingClass]:
        """Get all nodes with semantic_type Pharmacologic Substance"""
        graph = owl.default_world.as_rdflib_graph()
        query_str = '''SELECT ?x WHERE
        {
            ?x <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#P106>
            "Pharmacologic Substance"
        }'''
        typed_results = set(graph.query(query_str))

        retired_query_str = '''SELECT ?x WHERE {
            ?x <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#P310>
            "Retired_Concept"
        }
        '''

        retired_results = set(graph.query(retired_query_str))

        typed_results = typed_results - retired_results

        def parse_uri(uri):
            uri.toPython().split('#')[1]

        for result in typed_results:
            # parse result as URI and get ThingClass object back from NCIt
            class_object = ncit[result[0].toPython().split('#')[1]]
            uq_nodes.add(class_object)
        return uq_nodes

    def _add_meta(self, db: Session):
        meta_object = Meta(src_name=SourceName.NCIT.value,
                           data_license="CC BY 4.0",
                           data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa F401
                           version=self._version,
                           data_url=self._SRC_DIR)
        db.add(meta_object)  # TODO replace w/ boto3 load()

    def _transform_data(self, db: Session, *args, **kwargs):
        """Get data from file and construct objects for loading"""
        ncit = owl.get_ontology(self._data_src.absolute().as_uri())
        ncit.load()
        self._add_meta(db)
        uq_nodes = set()
        uq_nodes = self.get_desc_nodes(ncit.C1909, uq_nodes)
        uq_nodes = self.get_typed_nodes(uq_nodes, ncit)
        for node in uq_nodes:
            concept_id = f"{NamespacePrefix.NCIT.value}:{node.name}"
            if node.P108:
                label = node.P108.first()
            else:
                label = None
            aliases = [Alias()]
            node.P90
            if label and aliases and label in aliases:
                aliases.remove(label)

            other_ids = []
            if node.P207:
                other_id = OtherIdentifier(
                    concept_id=concept_id,
                    other_id=f"{NamespacePrefix.UMLS.value}:{node.P207.first()}"  # noqa F501
                )
                other_ids.append(other_id)
            if node.P210:
                other_id = OtherIdentifier(
                    concept_id=concept_id,
                    other_id=f"{NamespacePrefix.CASREGISTRY.value}:{node.P210.first()}"  # noqa F501
                )
                other_ids.append(other_id)
            if node.P319:
                other_id = OtherIdentifier(
                    concept_id=concept_id,
                    other_id=f"{NamespacePrefix.FDA.value}:{node.P319.first()}"
                )
                other_ids.append(other_id)
            if node.P320:
                other_id = OtherIdentifier(
                    concept_id=concept_id,
                    other_id=f"{NamespacePrefix.ISO.value}:{node.P320.first()}"
                )
                other_ids.append(other_id)

            therapy = Therapy(concept_id=concept_id,  # noqa F841
                              src_name=SourceName.NCIT.value,
                              label=label,
                              aliases=aliases,
                              other_identifiers=other_ids)

            # TODO call load() method

    def _download_data(self):
        logger.info('Downloading NCI Thesaurus...')
        url = self._SRC_DIR + self._SRC_FNAME
        out_dir = PROJECT_ROOT / 'data' / 'ncit'
        zip_path = out_dir / 'ncit.zip'
        response = requests.get(url, stream=True)
        handle = open(zip_path, "wb")
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                handle.write(chunk)
        print(zip_path)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(out_dir)
        remove(zip_path)
        version = self._SRC_DIR.split('/')[-2].split('_')[0]
        rename(out_dir / 'Thesaurus.owl', out_dir / f'ncit_{version}.owl')
        logger.info('Finished downloading NCI Thesaurus')

    def _extract_data(self, *args, **kwargs):
        """Get NCIt source file"""
        if 'data_path' in kwargs:
            self._data_src = kwargs['data_path']
        else:
            data_dir = PROJECT_ROOT / 'data' / 'ncit'
            data_dir.mkdir(exist_ok=True, parents=True)
            dir_files = list(data_dir.iterdir())
            if len(dir_files) == 0:
                self._download_data()
                dir_files = list(data_dir.iterdir())
            self._data_src = sorted(dir_files)[-1]
        self._version = self._data_src.stem.split('_')[1]
