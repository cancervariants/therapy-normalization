"""ETL methods for NCIt source"""
import logging
from typing import Set

import requests
import owlready2 as owl
from owlready2.entity import ThingClass

from therapy import DownloadException
from therapy.schemas import SourceName, NamespacePrefix, SourceMeta, RecordParams
from therapy.etl.base import Base

logger = logging.getLogger("therapy")
logger.setLevel(logging.DEBUG)


class NCIt(Base):
    """Class for NCIt ETL methods.

    Extracts:
     * NCIt classes with semantic_type "Pharmacologic Substance" but not
       Retired_Concept
     * NCIt classes that are subclasses of C1909 (Pharmacologic Substance)
    """

    def _download_data(self) -> None:
        """Download NCI thesaurus source file.
        The NCI directory structure can be a little tricky, so this method attempts to
        retrieve a file matching the latest version number from both the subdirectory
        root (where the current version is typically posted) as well as the year-by-year
        archives if that fails.
        """
        logger.info("Retrieving source data for NCIt")
        base_url = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus"
        # ping base NCIt directory
        release_fname = f"Thesaurus_{self._version}.OWL.zip"
        src_url = f"{base_url}/{release_fname}"
        r_try = requests.get(src_url)
        if r_try.status_code != 200:
            # ping NCIt archive directories
            archive_url = f"{base_url}/archive/{self._version}_Release/{release_fname}"
            archive_try = requests.get(archive_url)
            if archive_try.status_code != 200:
                old_archive_url = f"{base_url}/archive/20{self._version[0:2]}/{self._version}_Release/{release_fname}"  # noqa: E501
                old_archive_try = requests.get(old_archive_url)
                if old_archive_try.status_code != 200:
                    msg = (
                        f"NCIt download failed: tried {src_url}, {archive_url}, and "
                        f"{old_archive_url}"
                    )
                    logger.error(msg)
                    raise DownloadException(msg)
                else:
                    src_url = old_archive_url
            else:
                src_url = archive_url

        self._http_download(src_url, self._src_dir / f"ncit_{self._version}.owl",
                            handler=self._zip_handler)
        logger.info("Successfully retrieved source data for NCIt")

    def _get_desc_nodes(self, node: ThingClass,
                        uq_nodes: Set[ThingClass]) -> Set[ThingClass]:
        """Create set of unique subclasses of node parameter.
        Should be originally called on ncit:C1909: Pharmacologic Substance.

        :param owlready2.entity.ThingClass node: concept node to either retrieve
            descendants of, or to normalize and add to DB
        :param Set[owlready2.entity.ThingClass] uq_nodes: set of unique class nodes
            found so far from recursive tree exploration
        :return: the uq_nodes set, updated with any class nodes found from recursive
            exploration of this branch of the class tree
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

        :param Set[owlready2.entity.ThingClass] uq_nodes: set of unique class nodes
            found so far.
        :param owl.namespace.Ontology ncit: owlready2 Ontology instance for NCIt.
        :return: uq_nodes, with the addition of all classes found to have semantic_type
            Pharmacologic Substance and not of type Retired_Concept
        :rtype: Set[owlready2.entity.ThingClass]
        """
        graph = owl.default_world.as_rdflib_graph()

        query_str = """SELECT ?x WHERE {
            ?x <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#P106>
            \"Pharmacologic Substance\"
        }
        """
        typed_results = set(graph.query(query_str))

        retired_query_str = """SELECT ?x WHERE {
            ?x <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#P310>
            \"Retired_Concept\"
        }
        """
        retired_results = set(graph.query(retired_query_str))

        typed_results = {r for r in (typed_results - retired_results)
                         if r is not None}

        for result in typed_results:
            # parse result as URI and get ThingClass object back from NCIt
            class_object = ncit[result[0].toPython().split("#")[1]]
            uq_nodes.add(class_object)
        return uq_nodes

    def _transform_data(self) -> None:
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
            aliases = node.P90.copy()  # prevent CallbackList error
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
                if ":" in iri:
                    iri = iri.split(":")[1]
                associated_with.append(f"{NamespacePrefix.CHEBI.value}:{iri}")
            params: RecordParams = {  # type: ignore
                "concept_id": concept_id,
                "label": label,
                "aliases": aliases,
                "xrefs": xrefs,
                "associated_with": associated_with
            }
            self._load_therapy(params)

    def _load_meta(self) -> None:
        """Load metadata"""
        metadata = SourceMeta(
            data_license="CC BY 4.0",
            data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa F401
            version=self._version,
            data_url="https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/",
            rdp_url="http://reusabledata.org/ncit.html",
            data_license_attributes={
                "non_commercial": False,
                "share_alike": False,
                "attribution": True
            }
        )
        params = dict(metadata)
        params["src_name"] = SourceName.NCIT.value
        self.database.metadata.put_item(Item=params)
