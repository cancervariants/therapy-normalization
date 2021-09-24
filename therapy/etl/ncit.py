"""ETL methods for NCIt source"""
from .base import Base
from therapy import DownloadException
from therapy.schemas import SourceName, NamespacePrefix, Therapy, SourceMeta
import logging
import owlready2 as owl
from owlready2.entity import ThingClass
from typing import Set
import requests
import zipfile
import bioversions
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

    def _download_data(self):
        """Download NCI thesaurus source file for loading into normalizer."""
        base_dir_url = 'https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/'
        latest_ncit = bioversions.get_version('ncit')
        # ping base NCIt directory
        self._SRC_URL = f'{base_dir_url}{latest_ncit}_Release/Thesaurus_{latest_ncit}.OWL.zip'  # noqa: E501
        r_try = requests.get(self._SRC_URL)
        if r_try.status_code != 200:
            # ping NCIt archive directory
            archive_ncit_url = f'{base_dir_url}archive/20{latest_ncit[0:2]}/{latest_ncit}_Release/Thesaurus_{latest_ncit}.OWL.zip'  # noqa: E501
            archive_try = requests.get(archive_ncit_url)
            if archive_try.status_code != 200:
                msg = f'NCIt download failed: tried {self._SRC_URL} and {archive_ncit_url}'  # noqa: E501
                logger.error(msg)
                raise DownloadException(msg)
            self._SRC_URL = archive_ncit_url

        zip_path = self._src_data_dir / 'ncit.zip'
        logger.info('Downloading NCI Thesaurus...')
        response = requests.get(self._SRC_URL, stream=True)
        handle = open(zip_path, "wb")
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                handle.write(chunk)
        handle.close()
        logger.info('Finished downloading NCI Thesaurus')
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self._src_data_dir)
        remove(zip_path)
        rename(self._src_data_dir / 'Thesaurus.owl', self._src_data_dir / f'ncit_{latest_ncit}.owl')  # noqa: E501

    def _get_desc_nodes(self, node: ThingClass,
                        uq_nodes: Set[ThingClass]) -> Set[ThingClass]:
        """Create set of unique subclasses of node parameter.
        Should be originally called on ncit:C1909: Pharmacologic Substance.

        :param owlready2.entity.ThingClass node: concept node to either
            retrieve descendants of, or to normalize and add to DB
        :param Set[owlready2.entity.ThingClass] uq_nodes: set of unique class
            nodes found so far from recursive tree exploration
        :return: the uq_nodes set, updated with any class nodes found from
            recursive exploration of this branch of the class tree
        :rtype: Set[owlready2.entity.ThingClass]
        """
        children = node.descendants()
        if children:
            for child_node in children:
                if child_node is not node:
                    uq_nodes.add(child_node)
                    self._get_desc_nodes(child_node, uq_nodes)
        return uq_nodes

    def _get_typed_nodes(self, uq_nodes: Set[ThingClass],
                         ncit: owl.namespace.Ontology) -> Set[ThingClass]:
        """Get all nodes with semantic_type Pharmacologic Substance

        :param Set[owlready2.entity.ThingClass] uq_nodes: set of unique class
            nodes found so far.
        :param owl.namespace.Ontology ncit: owlready2 Ontology instance for
            NCI Thesaurus.
        :return: uq_nodes, with the addition of all classes found to have
            semantic_type Pharmacologic Substance and not of type
            Retired_Concept
        :rtype: Set[owlready2.entity.ThingClass]
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

    def _transform_data(self):
        """Get data from file and construct objects for loading"""
        ncit = owl.get_ontology(self._src_file.absolute().as_uri())
        ncit.load()
        uq_nodes = {ncit.C49236}  # add Therapeutic Procedure
        uq_nodes = self._get_desc_nodes(ncit.C1909, uq_nodes)
        uq_nodes = self._get_typed_nodes(uq_nodes, ncit)
        for node in uq_nodes:
            concept_id = f"{NamespacePrefix.NCIT.value}:{node.name}"
            if node.P108:
                label = node.P108.first()
            else:
                label = None
            aliases = node.P90
            if label and aliases and label in aliases:
                aliases.remove(label)

            xrefs = []
            associated_with = []
            if node.P207:
                associated_with.append(f"{NamespacePrefix.UMLS.value}:"
                                       f"{node.P207.first()}")
            if node.P210:
                xrefs.append(f"{NamespacePrefix.CASREGISTRY.value}:"
                             f"{node.P210.first()}")
            if node.P319:
                associated_with.append(f"{NamespacePrefix.UNII.value}:"
                                       f"{node.P319.first()}")
            if node.P320:
                associated_with.append(f"{NamespacePrefix.ISO.value}:"
                                       f"{node.P320.first()}")
            if node.P368:
                iri = node.P368.first()
                if ':' in iri:
                    iri = iri.split(':')[1]
                associated_with.append(f"{NamespacePrefix.CHEBI.value}:{iri}")
            params = {
                'concept_id': concept_id,
                'label': label,
                'aliases': aliases,
                'xrefs': xrefs,
                'associated_with': associated_with
            }
            assert Therapy(**params)
            self._load_therapy(params)

    def _load_meta(self):
        """Load metadata"""
        metadata = SourceMeta(data_license="CC BY 4.0",
                              data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa F401
                              version=self._version,
                              data_url=self._SRC_URL.split('Thesaurus_')[0],
                              rdp_url='http://reusabledata.org/ncit.html',
                              data_license_attributes={
                                  'non_commercial': False,
                                  'share_alike': False,
                                  'attribution': True
                              })
        params = dict(metadata)
        params['src_name'] = SourceName.NCIT.value
        self.database.metadata.put_item(Item=params)
