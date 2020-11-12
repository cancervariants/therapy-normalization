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


class NCIt(Base):
    """Core NCIt ETL class
    Extracting both:
     * NCIt classes with semantic_type "Pharmacologic Substance"
     * NCIt classes that are subclasses of C1909 (Pharmacologic Substance)
    """

    def __init__(self, *args, **kwargs):
        """Override base class init method. Call ETL methods with generated
        db SessionLocal instance.
        """
        B.metadata.create_all(bind=engine)
        self._extract_data()
        db: Session = SessionLocal()
        self._transform_data(db)
        db.commit()
        db.close()

    def _load_data(self, db: Session, leaf: ThingClass):
        """Load data from individual NCIt entry into db"""
        concept_id = f"{NamespacePrefix.NCIT.value}:{leaf.name}"
        if leaf.P108:
            label = leaf.P108.first()
        else:
            label = None
        aliases = leaf.P90
        if label and aliases and label in aliases:
            aliases.remove(label)
        therapy = Therapy(
            concept_id=concept_id,
            src_name=SourceName.NCIT.value,
            label=label,
        )
        db.add(therapy)

        if leaf.P207:
            other_id = OtherIdentifier(
                concept_id=concept_id,
                other_id=f"{NamespacePrefix.UMLS.value}:{leaf.P207.first()}"
            )
            db.add(other_id)
        if leaf.P210:
            other_id = OtherIdentifier(
                concept_id=concept_id,
                other_id=f"{NamespacePrefix.CASREGISTRY.value}:{leaf.P210.first()}"  # noqa F501
            )
            db.add(other_id)
        if leaf.P319:
            other_id = OtherIdentifier(
                concept_id=concept_id,
                other_id=f"{NamespacePrefix.FDA.value}:{leaf.P319.first()}"
            )
            db.add(other_id)
        if leaf.P320:
            other_id = OtherIdentifier(
                concept_id=concept_id,
                other_id=f"{NamespacePrefix.ISO.value}:{leaf.P320.first()}"
            )
            db.add(other_id)

        for a in aliases:
            alias = Alias(alias=a, concept_id=concept_id)
            db.add(alias)

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
                           data_url="https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/",)  # noqa F401
        db.add(meta_object)

    def _transform_data(self, db: Session, *args, **kwargs):
        """Get data from file and construct objects for loading"""
        ncit = owl.get_ontology(self._data_src.absolute().as_uri())
        ncit.load()
        self._add_meta(db)
        uq_nodes = set()
        uq_nodes = self.get_desc_nodes(ncit.C1909, uq_nodes)
        uq_nodes = self.get_typed_nodes(uq_nodes, ncit)
        for node in uq_nodes:
            self._load_data(db, node)

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
