"""ETL methods for NCIt source"""
from .base import Base
from therapy import PROJECT_ROOT
from therapy.schemas import SourceName, NamespacePrefix, Therapy, Meta
from therapy.database import DYNAMODB, THERAPIES_TABLE, METADATA_TABLE
import logging
import owlready2 as owl
from owlready2.entity import ThingClass
from typing import Set
import requests
import zipfile
from os import remove, rename

logger = logging.getLogger('therapy')
logger.setLevel(logging.DEBUG)


class NCIt(Base):
    """Core NCIt ETL class.

    Extracting both:
     * NCIt classes with semantic_type "Pharmacologic Substance" but not
       Retired_Concept
     * NCIt classes that are subclasses of C1909 (Pharmacologic Substance)
    """

    def __init__(self,
                 src_dir: str = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/",  # noqa F401
                 src_fname: str = "Thesaurus_20.09d.OWL.zip",
                 *args,
                 **kwargs):
        """Override base class init method. Call ETL methods.

        Args:
            src_dir: URL of source directory
            src_fname: filename for source file within source directory URL
        """
        self._SRC_DIR = src_dir
        self._SRC_FNAME = src_fname
        self._extract_data()
        self.db = DYNAMODB
        self._transform_data()

    def _download_data(self):
        """Download NCI thesaurus source file for loading into normalizer"""
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

    def get_desc_nodes(self, node: ThingClass,
                       uq_nodes: Set[ThingClass]) -> Set[ThingClass]:
        """Create set of unique subclasses of node parameter.
        Should be originally called on ncit:C1909: Pharmacologic Substance.

        Args:
            node: concept node to either retrieve descendants of, or to
                normalize and add to DB
            uq_nodes: set of unique class nodes found so far from recursive
                tree exploration

        Returns:
            the uq_nodes set, updated with any class nodes found from
                recursive exploration of this branch of the class tree
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
        """Get all nodes with semantic_type Pharmacologic Substance

        Args:
            uq_nodes: set of unique class nodes found so far.
            ncit: owlready2 Ontology instance for NCI Thesaurus

        Returns:
            uq_nodes, with the addition of all classes found to have
                semantic_type Pharmacologic Substance and not of type
                Retired_Concept
        """
        graph = owl.default_world.as_rdflib_graph()

        query_str = '''SELECT ?x WHERE {
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

        for result in typed_results:
            # parse result as URI and get ThingClass object back from NCIt
            class_object = ncit[result[0].toPython().split('#')[1]]
            uq_nodes.add(class_object)
        return uq_nodes

    def _transform_data(self, *args, **kwargs):
        """Get data from file and construct objects for loading"""
        ncit = owl.get_ontology(self._data_src.absolute().as_uri())
        ncit.load()
        self._add_meta()
        uq_nodes = set()
        uq_nodes = self.get_desc_nodes(ncit.C1909, uq_nodes)
        uq_nodes = self.get_typed_nodes(uq_nodes, ncit)
        with THERAPIES_TABLE.batch_writer() as batch:  # noqa: E501
            for node in uq_nodes:
                concept_id = f"{NamespacePrefix.NCIT.value}:{node.name}"
                if node.P108:
                    label = node.P108.first()
                else:
                    label = None
                aliases = node.P90
                if label and aliases and label in aliases:
                    aliases.remove(label)

                other_ids = []
                if node.P207:
                    other_ids.append(f"{NamespacePrefix.UMLS.value}:{node.P207.first()}")  # noqa: E501
                if node.P210:
                    other_ids.append(f"{NamespacePrefix.CASREGISTRY.value}:{node.P210.first()}")  # noqa: E501
                if node.P319:
                    other_ids.append(f"{NamespacePrefix.FDA.value}:{node.P319.first()}")  # noqa: E501
                if node.P320:
                    other_ids.append(f"{NamespacePrefix.ISO.value}:{node.P320.first()}")  # noqa: E501

                therapy = Therapy(concept_id=concept_id,  # noqa: F841
                                  src_name=SourceName.NCIT.value,
                                  label=label,
                                  aliases=aliases,
                                  other_identifiers=other_ids)
                self._load_therapy(therapy, batch)

    def _add_meta(self):
        """Load metadata"""
        metadata = Meta(data_license="CC BY 4.0",
                        data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa F401
                        version=self._version,
                        data_url=self._SRC_DIR)
        METADATA_TABLE.put_item(Item={
            'src_name': SourceName.NCIT.value,
            'data_license': metadata.data_license,
            'data_license_url': metadata.data_license_url,
            'version': metadata.version,
            'data_url': metadata.data_url
        })

    def _load_therapy(self, therapy: Therapy, batch):
        """Load individual therapy into dynamodb table

        Args:
            therapy: complete Therapy instance
            batch: dynamoDB batch_writer context manager object
        """
        item = therapy.dict()
        concept_id_lower = item['concept_id'].lower()
        aliases = {alias.lower() for alias in item['aliases']}
        for alias in aliases:
            pk = f"{alias}##alias"
            batch.put_item(Item={
                'label_and_type': pk,
                'concept_id': concept_id_lower,
                'src_name': SourceName.NCIT.value
            })
        if item['label']:
            pk = f"{item['label'].lower()}##label"
            batch.put_item(Item={
                'label_and_type': pk,
                'concept_id': concept_id_lower,
                'src_name': SourceName.NCIT.value
            })
        else:
            del therapy['label']
        item['label_and_type'] = f"{concept_id_lower}##identity"
        item['src_name'] = SourceName.NCIT.value

        batch.put_item(Item=item)
