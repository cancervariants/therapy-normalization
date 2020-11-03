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


class NCIt(Base):
    """Core NCIt ETL class
    Notes:
    * <NHCO> = NCIT concept ID
    * <A8> = Concept Is In Subset
    * <P90> = synonym (contains string, term type, source, optional source
        code)
    * <P97> = definition
    * <P106> = semantic type, a property that represents a description of the
        sort of thing or category to which a concept belongs
    * <P107> = display name
    * <P108> = preferred name
    * <P207> = NLM concept ID
    * <P208> = concept ID for concepts that are in NCIt but not NLM UMLS (???)
    * <P325> =
    * <P378> =
    * <P383> =
    * <P384> =
    * <C1909> = Pharmacologic Substance


    concept id:
        NHC0
    label:
        P107?
        P108?
    aliases:
        P90 -- not all?

    other identifiers:
        NLM: P207
        uncl.: P208 ("for concepts in NCIT but not NLM"?)
        CAS: P210
        ISO: P320
        FDA: P319

    (no max phase or withdrawn)

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

    def get_unique_nodes(self, node: ThingClass, nodes: set):
        """Create set of unique leaf nodes"""
        children = node.descendants()
        if children:
            for child_node in children:
                if child_node is not node:
                    nodes.add(child_node)
                    self.get_unique_nodes(child_node, nodes)
        return nodes

    def _add_meta(self, db: Session):
        meta_object = Meta(src_name=SourceName.NCIT.value,
                           data_license="CC BY 4.0",
                           data_license_url="https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa F401
                           version=self._version,
                           data_url="https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/Thesaurus_20.09d.OWL.zip",)  # noqa F401
        db.add(meta_object)

    def _transform_data(self, db: Session, *args, **kwargs):
        """Get data from file and construct objects for loading"""
        ncit = owl.get_ontology(self._data_src.absolute().as_uri())
        ncit.load()
        self._add_meta(db)
        nodes = self.get_unique_nodes(ncit.C1909, set())
        for node in nodes:
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
