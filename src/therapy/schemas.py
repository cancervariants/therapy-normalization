"""Contains data models for representing VICC therapy records."""

from datetime import datetime
from enum import Enum, IntEnum
from types import MappingProxyType
from typing import Any, Literal

from ga4gh.core.models import MappableConcept
from pydantic import BaseModel, ConfigDict, StrictBool, constr

from therapy import __version__

# Working structure for object in preparation for upload to DB
RecordParams = dict[str, list | set | str | dict[str, Any]]


class ApprovalRating(str, Enum):
    """Define string constraints for approval rating attribute. This reflects a drug's
    regulatory approval status as evaluated by an individual source. We opted to retain
    each individual source's rating as a distinct enum value after finding that some
    sources disagreed on the true value (either because of differences in scope like
    US vs EU/other regulatory arenas, old or conflicting data, or other reasons). Value
    descriptions are provided below from each listed source.

    ChEMBL: [http://chembl.blogspot.com/2023/03/what-is-max-phase-in-chembl.html]
     - CHEMBL_NULL: "preclinical compounds with bioactivity data"
     - CHEMBL_0_5: "A clinical candidate drug in Early Phase 1 Clinical Trials e.g.
    CITRULLINE MALATE (CHEMBL4297667) is under clinical investigation for coronary
     - CHEMBL_1: "A clinical candidate drug in Phase 1 Clinical Trials e.g.
    SALCAPROZATE SODIUM (CHEMBL2107027) is under clinical investigation for treatment
    of diabetes mellitus. Note that this category also includes a small number of
    trials that are defined by ClinicalTrials.gov as "Phase 1/Phase 2"."
    artery disease at Early Phase 1."
     - CHEMBL_2: "A clinical candidate drug in Phase 2 Clinical Trials e.g.
    NEVANIMIBE HYDROCHLORIDE (CHEMBL542103) is under clinical investigation for
    treatment of Cushing syndrome at Phase 2. Note that this category also includes a
    small number of trials that are defined by ClinicalTrials.gov as "Phase 2/Phase 3"."
     - CHEMBL_3: "A clinical candidate drug in Phase 3 Clinical Trials e.g.
    TEGOPRAZAN (CHEMBL4297583) is under clinical investigation for treatment of peptic
    ulcer at Phase 3, and also liver disease at Phase 1. "
     - CHEMBL_4: "A marketed drug e.g. AMINOPHYLLINE (CHEMBL1370561) is an FDA
    approved drug for treatment of asthma. "
    https://pubs.acs.org/doi/10.1021/acs.chemrestox.0c00296
     - note: "Unknown (-1)" should simply be left as None, i.e. not assigned an
    `ApprovalRating`.

    Drugs@FDA:
     - FDA_PRESCRIPTION: "A prescription drug product requires a doctor's authorization
    to purchase."
     - FDA_OTC: "FDA defines OTC drugs as safe and effective for use by the general
    public without a doctor's prescription."
     - FDA_DISCONTINUED: "approved products that have never been marketed, have been
    discontinued from marketing, are for military use, are for export only, or have had
    their approvals withdrawn for reasons other than safety or efficacy after being
    discontinued from marketing"
     - FDA_TENTATIVE: "If a generic drug product is ready for approval before the
    expiration of any patents or exclusivities accorded to the reference listed drug
    product, FDA issues a tentative approval letter to the applicant.  FDA delays final
    approval of the generic drug product until all patent or exclusivity issues have
    been resolved."
    https://www.fda.gov/drugs/drug-approvals-and-databases/drugsfda-glossary-terms

    HemOnc.org:
    - HEMONC_APPROVED: Inferred by us if "Was FDA Approved Yr" property is present
    (described as "Year of FDA approval")
    https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6697579/

    Guide to Pharmacology:
    - GTOPDB_APPROVED: "Indicates pharmacologicaly active substances, specified by their
    INNs, that have been approved for clinical use by a regulatory agency, typically the
    FDA, EMA or in Japan. This classification persists regardless of whether the drug
    may later have been withdrawn or discontinued. (N.B. in some cases the information
    on approval status was obtained indirectly via databases such as Drugbank.)"
    - GTOPDB_WITHDRAWN: "No longer approved for its original clinical use, as notified
    by the FDA, typically as a consequence of safety or side effect issues."
    https://www.guidetopharmacology.org/helpPage.jsp

    RxNorm:
    - RXNORM_PRESCRIBABLE: "The RxNorm Current Prescribable Content is a subset of
    currently prescribable drugs found in RxNorm. We intend it to be an approximation of
    the prescription drugs currently marketed in the US. The subset also includes many
    over-the-counter drugs."
    https://www.nlm.nih.gov/research/umls/rxnorm/docs/prescribe.html
    """

    CHEMBL_NULL = "chembl_phase_null"
    CHEMBL_0_5 = "chembl_phase_0.5"
    CHEMBL_1 = "chembl_phase_1"
    CHEMBL_2 = "chembl_phase_2"
    CHEMBL_3 = "chembl_phase_3"
    CHEMBL_4 = "chembl_phase_4"
    CHEMBL_WITHDRAWN = "chembl_withdrawn"
    FDA_OTC = "fda_otc"
    FDA_PRESCRIPTION = "fda_prescription"
    FDA_DISCONTINUED = "fda_discontinued"
    FDA_TENTATIVE = "fda_tentative"
    HEMONC_APPROVED = "hemonc_approved"
    GTOPDB_APPROVED = "gtopdb_approved"
    GTOPDB_WITHDRAWN = "gtopdb_withdrawn"
    RXNORM_PRESCRIBABLE = "rxnorm_prescribable"


class HasIndication(BaseModel):
    """Data regarding disease indications from regulatory bodies."""

    disease_id: str
    disease_label: str
    normalized_disease_id: str | None = None
    supplemental_info: dict[str, str | None] | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "disease_id": "mesh:D016778",
                "disease_label": "Malaria, Falciparum",
                "normalized_disease_id": "ncit:C34798",
                "supplemental_info": {"chembl_max_phase_for_ind": "chembl_phase_2"},
            }
        }
    )


class Therapy(BaseModel):
    """A pharmacologic substance used to treat a medical condition."""

    concept_id: str
    label: str | None = None
    aliases: list[str] | None = []
    trade_names: list[str] | None = []
    xrefs: list[str] | None = []
    associated_with: list[str] | None = []
    approval_ratings: list[ApprovalRating] | None = None
    approval_year: list[str] | None = []
    has_indication: list[HasIndication] | None = []

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "label": "CISPLATIN",
                "concept_id": "chembl:CHEMBL11359",
                "aliases": [
                    "Cisplatin",
                    "Cis-Platinum II",
                    "Cisplatinum",
                    "cis-diamminedichloroplatinum(II)",
                    "CIS-DDP",
                    "INT-230-6 COMPONENT CISPLATIN",
                    "INT230-6 COMPONENT CISPLATIN",
                    "NSC-119875",
                    "Platinol",
                    "Platinol-Aq",
                ],
                "xrefs": [],
                "associated_with": None,
                "approval_ratings": "approved",
                "approval_year": [],
                "has_indication": [],
                "trade_names": ["PLATINOL", "PLATINOL-AQ", "CISPLATIN"],
            }
        }
    )


class RecordType(str, Enum):
    """Record item types."""

    IDENTITY = "identity"
    MERGER = "merger"


class RefType(str, Enum):
    """Reference item types."""

    # Must be in descending MatchType order.
    LABEL = "label"
    TRADE_NAMES = "trade_name"
    ALIASES = "alias"
    XREFS = "xref"
    ASSOCIATED_WITH = "associated_with"


# not incorporated as a RefType because it shouldn't be publicly searchable
RXNORM_BRAND_ITEM_TYPE = "rx_brand"


class MatchType(IntEnum):
    """Define string constraints for use in Match Type attributes."""

    CONCEPT_ID = 100
    LABEL = 80
    TRADE_NAME = 80
    ALIAS = 60
    XREF = 60
    ASSOCIATED_WITH = 60
    FUZZY_MATCH = 20
    NO_MATCH = 0


class SourcePriority(IntEnum):
    """Define constraints for Source Priority Rankings."""

    RXNORM = 1
    NCIT = 2
    HEMONC = 3
    DRUGBANK = 4
    DRUGSATFDA = 5
    GUIDETOPHARMACOLOGY = 6
    CHEMBL = 7
    CHEMIDPLUS = 8
    WIKIDATA = 9


class SourceName(str, Enum):
    """Define string constraints to ensure consistent capitalization."""

    WIKIDATA = "Wikidata"
    CHEMBL = "ChEMBL"
    NCIT = "NCIt"
    DRUGBANK = "DrugBank"
    CHEMIDPLUS = "ChemIDplus"
    RXNORM = "RxNorm"
    HEMONC = "HemOnc"
    DRUGSATFDA = "DrugsAtFDA"
    DRUGSATFDA_NDA = DRUGSATFDA
    DRUGSATFDA_ANDA = DRUGSATFDA  # noqa: PIE796
    GUIDETOPHARMACOLOGY = "GuideToPHARMACOLOGY"


class NamespacePrefix(Enum):
    """Define string constraints for namespace prefixes on concept IDs."""

    ATC = "atc"  # Anatomical Therapeutic Chemical Classification System
    CHEBI = "CHEBI"
    CHEMBL = "chembl"
    CHEMIDPLUS = "chemidplus"
    CASREGISTRY = CHEMIDPLUS
    CVX = "cvx"  # Vaccines Administered
    DRUGBANK = "drugbank"
    DRUGCENTRAL = "drugcentral"
    DRUGSATFDA_ANDA = "drugsatfda.anda"
    DRUGSATFDA_NDA = "drugsatfda.nda"
    HCPCS = "hcpcs"  # Healthcare Common Procedure Coding System
    HEMONC = "hemonc"
    INCHIKEY = "inchikey"
    IUPHAR_LIGAND = "iuphar.ligand"
    GUIDETOPHARMACOLOGY = IUPHAR_LIGAND
    MMSL = "mmsl"  # Multum MediSource Lexicon
    MSH = "mesh"  # Medical Subject Headings
    NCIT = "ncit"
    NDC = "ndc"  # National Drug Code
    PUBCHEMCOMPOUND = "pubchem.compound"
    PUBCHEMSUBSTANCE = "pubchem.substance"
    RXNORM = "rxcui"
    SPL = "spl"  # Structured Product Labeling
    UMLS = "umls"
    UNII = "unii"
    UNIPROT = "uniprot"
    USP = "usp"  # USP Compendial Nomenclature
    VANDF = "vandf"  # Veterans Health Administration National Drug File
    WIKIDATA = "wikidata"


# Source to URI. Will use system prefix URL, OBO Foundry persistent URL (PURL) or source homepage
NAMESPACE_TO_SYSTEM_URI: MappingProxyType[NamespacePrefix, str] = MappingProxyType(
    {
        NamespacePrefix.ATC: "https://atcddd.fhi.no/atc_ddd_index/?code=",
        NamespacePrefix.CHEBI: "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=",
        NamespacePrefix.CHEMBL: "https://www.ebi.ac.uk/chembl/explore/compound/",
        NamespacePrefix.CHEMIDPLUS: "https://commonchemistry.cas.org/detail?cas_rn=",
        NamespacePrefix.CASREGISTRY: "https://commonchemistry.cas.org/detail?cas_rn=",
        NamespacePrefix.CVX: "https://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=cvx",
        NamespacePrefix.DRUGBANK: "https://go.drugbank.com/drugs/",
        NamespacePrefix.DRUGCENTRAL: "https://drugcentral.org/drugcard/",
        NamespacePrefix.DRUGSATFDA_ANDA: "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
        NamespacePrefix.DRUGSATFDA_NDA: "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
        NamespacePrefix.HEMONC: "https://hemonc.org",
        NamespacePrefix.HCPCS: "https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system",
        NamespacePrefix.INCHIKEY: "https://www.chemspider.com",
        NamespacePrefix.IUPHAR_LIGAND: "https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?ligandId=",
        NamespacePrefix.GUIDETOPHARMACOLOGY: "https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?ligandId=",
        NamespacePrefix.MMSL: "https://www.nlm.nih.gov/research/umls/rxnorm/sourcereleasedocs/mmsl.html",
        NamespacePrefix.MSH: "https://id.nlm.nih.gov/mesh/",
        NamespacePrefix.NCIT: "https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=",
        NamespacePrefix.NDC: "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
        NamespacePrefix.PUBCHEMCOMPOUND: "https://pubchem.ncbi.nlm.nih.gov/compound/",
        NamespacePrefix.PUBCHEMSUBSTANCE: "https://pubchem.ncbi.nlm.nih.gov/substance/",
        NamespacePrefix.RXNORM: "https://mor.nlm.nih.gov/RxNav/search?searchBy=RXCUI&searchTerm=",
        NamespacePrefix.SPL: "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
        NamespacePrefix.UMLS: "https://uts.nlm.nih.gov/uts/umls/concept/",
        NamespacePrefix.UNII: "https://precision.fda.gov/uniisearch/srs/unii/",
        NamespacePrefix.UNIPROT: "http://purl.uniprot.org/uniprot/",
        NamespacePrefix.USP: "https://www.usp.org/health-quality-safety/compendial-nomenclature",
        NamespacePrefix.VANDF: "https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/VANDF",
        NamespacePrefix.WIKIDATA: "https://www.wikidata.org/wiki/",
    }
)

# URI to source
SYSTEM_URI_TO_NAMESPACE = {
    system_uri: ns.value for ns, system_uri in NAMESPACE_TO_SYSTEM_URI.items()
}


class DataLicenseAttributes(BaseModel):
    """Define constraints for data license attributes."""

    non_commercial: StrictBool
    share_alike: StrictBool
    attribution: StrictBool


class ItemTypes(str, Enum):
    """Item types used in DynamoDB."""

    # Must be in descending MatchType order.
    LABEL = "label"
    TRADE_NAMES = "trade_name"
    ALIASES = "alias"
    XREFS = "xref"
    ASSOCIATED_WITH = "associated_with"


class SourceMeta(BaseModel):
    """Metadata for a given source to return in response object."""

    data_license: str
    data_license_url: str
    version: str
    data_url: str | None = None
    rdp_url: str | None = None
    data_license_attributes: dict[str, StrictBool]

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "data_license": "CC BY-SA 3.0",
                "data_license_url": "https://creativecommons.org/licenses/by-sa/3.0/",
                "version": "27",
                "data_url": "http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/",
                "rdp_url": "http://reusabledata.org/chembl.html",
                "data_license_attributes": {
                    "non_commercial": False,
                    "share_alike": True,
                    "attribution": True,
                },
            }
        }
    )


class SourceSearchMatches(BaseModel):
    """Container for matching information from an individual source.
    Used when matches are requested as an object, not an array.
    """

    match_type: MatchType
    records: list[Therapy]
    source_meta_: SourceMeta

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "match_type": 80,
                "records": [
                    {
                        "concept_id": "chemidplus:15663-27-1",
                        "label": "Cisplatin",
                        "aliases": [
                            "1,2-Diaminocyclohexaneplatinum II citrate",
                            "cis-Diaminedichloroplatinum",
                        ],
                        "trade_names": [],
                        "xrefs": ["drugbank:DB00515"],
                        "associated_with": ["unii:Q20Q21Q62J"],
                        "approval_year": [],
                        "has_indication": [],
                    }
                ],
                "source_meta_": {
                    "data_license": "custom",
                    "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",
                    "version": "20230222",
                    "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                    "data_license_attributes": {
                        "non_commercial": False,
                        "attribution": True,
                        "share_alike": False,
                    },
                },
            }
        }
    )


class ServiceMeta(BaseModel):
    """Metadata regarding the therapy-normalization service."""

    name: Literal["thera-py"] = "thera-py"
    version: Literal[__version__] = __version__  # type: ignore[valid-type]
    response_datetime: datetime
    url: Literal["https://github.com/cancervariants/therapy-normalization"] = (
        "https://github.com/cancervariants/therapy-normalization"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "thera-py",
                "version": __version__,
                "response_datetime": "2021-04-05T16:44:15.367831",
                "url": "https://github.com/cancervariants/therapy-normalization",
            }
        }
    )


class MatchesNormalized(BaseModel):
    """Matches associated with normalized concept from a single source."""

    records: list[Therapy]
    source_meta_: SourceMeta


class BaseNormalizationService(BaseModel):
    """Base method providing shared attributes to Normalization service classes."""

    query: str
    warnings: list[dict] = []
    match_type: MatchType
    service_meta_: ServiceMeta


class UnmergedNormalizationService(BaseNormalizationService):
    """Response providing source records corresponding to normalization of user query.
    Enables retrieval of normalized concept while retaining sourcing for accompanying
    attributes.
    """

    normalized_concept_id: constr(pattern="^\\w[^:]*:.+$") | None = None  # type: ignore[valid-type]
    source_matches: dict[SourceName, MatchesNormalized]

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "query": "L745870",
                "warnings": [],
                "match_type": 80,
                "service_meta_": {
                    "response_datetime": "2022-04-22T11:40:18.921859",
                    "name": "thera-py",
                    "version": __version__,
                    "url": "https://github.com/cancervariants/therapy-normalization",
                },
                "normalized_concept_id": "iuphar.ligand:3303",
                "source_matches": {
                    "GuideToPHARMACOLOGY": {
                        "records": [
                            {
                                "concept_id": "iuphar.ligand:3303",
                                "label": "L745870",
                                "aliases": [
                                    "L-745,870",
                                    "L 745870",
                                    "3-[[4-(4-chlorophenyl)piperazin-1-yl]methyl]-1H-pyrrolo[2,3-b]pyridine",
                                ],
                                "trade_names": [],
                                "xrefs": [
                                    "chemidplus:158985-00-3",
                                    "chembl:CHEMBL267014",
                                ],
                                "associated_with": [
                                    "pubchem.substance:178100340",
                                    "pubchem.compound:5311200",
                                    "inchikey:OGJGQVFWEPNYSB-UHFFFAOYSA-N",
                                ],
                                "approval_ratings": None,
                                "approval_year": [],
                                "has_indication": [],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY-SA 4.0",
                            "data_license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
                            "version": "2021.4",
                            "data_url": "https://www.guidetopharmacology.org/download.jsp",
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "share_alike": True,
                                "attribution": True,
                            },
                        },
                    },
                    "ChEMBL": {
                        "records": [
                            {
                                "concept_id": "chembl:CHEMBL267014",
                                "label": "L-745870",
                                "aliases": [],
                                "trade_names": [],
                                "xrefs": [],
                                "associated_with": [],
                                "approval_ratings": ["chembl_phase_0"],
                                "approval_year": [],
                                "has_indication": [],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY-SA 3.0",
                            "data_license_url": "https://creativecommons.org/licenses/by-sa/3.0/",
                            "version": "29",
                            "data_url": "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_29",
                            "rdp_url": "http://reusabledata.org/chembl.html",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "share_alike": True,
                                "attribution": True,
                            },
                        },
                    },
                },
            }
        }
    )


class NormalizationService(BaseNormalizationService):
    """Response containing one or more merged records and source data."""

    therapy: MappableConcept | None = None
    source_meta_: dict[SourceName, SourceMeta] | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "query": "cisplatin",
                "warnings": None,
                "match_type": 80,
                "therapy": {
                    "conceptType": "Therapy",
                    "primaryCoding": {
                        "id": "rxcui:2555",
                        "code": "2555",
                        "system": "https://mor.nlm.nih.gov/RxNav/search?searchBy=RXCUI&searchTerm=",
                    },
                    "id": "normalize.therapy.rxcui:2555",
                    "name": "cisplatin",
                    "mappings": [
                        {
                            "coding": {
                                "id": "ncit:C376",
                                "system": "https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&code=",
                                "code": "C376",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "hemonc:105",
                                "system": "https://hemonc.org",
                                "code": "105",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugbank:DB00515",
                                "system": "https://go.drugbank.com/drugs/",
                                "code": "DB00515",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugbank:DB12117",
                                "system": "https://go.drugbank.com/drugs/",
                                "code": "DB12117",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugsatfda.anda:074656",
                                "system": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
                                "code": "074656",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugsatfda.anda:074735",
                                "system": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
                                "code": "074735",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugsatfda.anda:075036",
                                "system": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
                                "code": "075036",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugsatfda.anda:206774",
                                "system": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
                                "code": "206774",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugsatfda.anda:207323",
                                "system": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
                                "code": "207323",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "drugsatfda.nda:018057",
                                "system": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=",
                                "code": "018057",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "iuphar.ligand:5343",
                                "system": "https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?ligandId=",
                                "code": "5343",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "chembl:CHEMBL11359",
                                "system": "https://www.ebi.ac.uk/chembl/explore/compound/",
                                "code": "CHEMBL11359",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "chemidplus:15663-27-1",
                                "system": "https://commonchemistry.cas.org/detail?cas_rn=",
                                "code": "15663-27-1",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "wikidata:Q412415",
                                "system": "https://www.wikidata.org/wiki/",
                                "code": "Q412415",
                            },
                            "relation": "exactMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:0703-5748",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "0703-5748",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:63323-103",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "63323-103",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:70860-206",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "70860-206",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:44567-509",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "44567-509",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "usp:m17910",
                                "system": "https://www.usp.org/health-quality-safety/compendial-nomenclature",
                                "code": "m17910",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:44567-530",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "44567-530",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "pubchem.substance:178102005",
                                "system": "https://pubchem.ncbi.nlm.nih.gov/substance/",
                                "code": "178102005",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:9b008181-ab66-db2f-e053-2995a90aad57",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "9b008181-ab66-db2f-e053-2995a90aad57",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:16729-288",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "16729-288",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:a66eda32-1164-439a-ac8e-73138365ec06",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "a66eda32-1164-439a-ac8e-73138365ec06",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:dd45d777-d4c1-40ee-b4f0-c9e001a15a8c",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "dd45d777-d4c1-40ee-b4f0-c9e001a15a8c",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "CHEBI:27899",
                                "system": "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=",
                                "code": "CHEBI:27899",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:44567-511",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "44567-511",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:0703-5747",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "0703-5747",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:68083-162",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "68083-162",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "atc:L01XA01",
                                "system": "https://atcddd.fhi.no/atc_ddd_index/?code=",
                                "code": "L01XA01",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:44567-510",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "44567-510",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "mmsl:31747",
                                "system": "https://www.nlm.nih.gov/research/umls/rxnorm/sourcereleasedocs/mmsl.html",
                                "code": "31747",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:2c569ef0-588f-4828-8b2d-03a2120c9b4c",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "2c569ef0-588f-4828-8b2d-03a2120c9b4c",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "vandf:4018139",
                                "system": "https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/VANDF",
                                "code": "4018139",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L",
                                "system": "https://www.chemspider.com",
                                "code": "LXZZYRPGZAFOLE-UHFFFAOYSA-L",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:68083-163",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "68083-163",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "inchikey:MOTIYCLHZZLHHQ-UHFFFAOYSA-N",
                                "system": "https://www.chemspider.com",
                                "code": "MOTIYCLHZZLHHQ-UHFFFAOYSA-N",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:adf5773e-9095-4cb4-a90f-72cbf82f4493",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "adf5773e-9095-4cb4-a90f-72cbf82f4493",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "mmsl:d00195",
                                "system": "https://www.nlm.nih.gov/research/umls/rxnorm/sourcereleasedocs/mmsl.html",
                                "code": "d00195",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:01c7a680-ee0d-42da-85e8-8d56c6fe7006",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "01c7a680-ee0d-42da-85e8-8d56c6fe7006",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:c43de769-d6d8-3bb9-e053-2995a90a5aa2",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "c43de769-d6d8-3bb9-e053-2995a90a5aa2",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "unii:H8MTN7XVC2",
                                "system": "https://precision.fda.gov/uniisearch/srs/unii/",
                                "code": "H8MTN7XVC2",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:68001-283",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "68001-283",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "unii:Q20Q21Q62J",
                                "system": "https://precision.fda.gov/uniisearch/srs/unii/",
                                "code": "Q20Q21Q62J",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "pubchem.compound:5702198",
                                "system": "https://pubchem.ncbi.nlm.nih.gov/compound/",
                                "code": "5702198",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "pubchem.compound:441203",
                                "system": "https://pubchem.ncbi.nlm.nih.gov/compound/",
                                "code": "441203",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:0143-9505",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "0143-9505",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:c3ddc4a5-9f1b-a8ee-e053-2a95a90a2265",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "c3ddc4a5-9f1b-a8ee-e053-2a95a90a2265",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "mmsl:4456",
                                "system": "https://www.nlm.nih.gov/research/umls/rxnorm/sourcereleasedocs/mmsl.html",
                                "code": "4456",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "umls:C0008838",
                                "system": "https://uts.nlm.nih.gov/uts/umls/concept/",
                                "code": "C0008838",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:0219ee77-eb38-b81f-e063-6394a90a5ca4",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "0219ee77-eb38-b81f-e063-6394a90a5ca4",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:89007399-3c34-40d2-8068-a0b8e09cbef8",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "89007399-3c34-40d2-8068-a0b8e09cbef8",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "spl:64bcce1a-6e31-4e73-8da5-11aa9e890da2",
                                "system": "https://www.fda.gov/industry/fda-data-standards-advisory-board/structured-product-labeling-resources",
                                "code": "64bcce1a-6e31-4e73-8da5-11aa9e890da2",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:72266-253",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "72266-253",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:25021-253",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "25021-253",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:72266-252",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "72266-252",
                            },
                            "relation": "relatedMatch",
                        },
                        {
                            "coding": {
                                "id": "ndc:0143-9504",
                                "system": "https://dps.fda.gov/ndc/searchresult?selection=finished_product&content=PRODUCTNDC&type=",
                                "code": "0143-9504",
                            },
                            "relation": "relatedMatch",
                        },
                    ],
                    "aliases": [
                        "CIS-DDP",
                        "cis Platinum",
                        "DDP",
                        "Dichlorodiammineplatinum",
                        "1,2-Diaminocyclohexaneplatinum II citrate",
                        "CISplatin",
                        "cis Diamminedichloroplatinum",
                        "CDDP",
                        "Diamminodichloride, Platinum",
                        "cis-Dichlorodiammineplatinum(II)",
                        "cis-Platinum",
                        "cis-diamminedichloroplatinum(II)",
                        "cis-Diamminedichloroplatinum(II)",
                        "Cis-DDP",
                        "cis-Diamminedichloroplatinum",
                        "cis-Diaminedichloroplatinum",
                        "Platinol-AQ",
                        "Platinol",
                        "Platinum Diamminodichloride",
                    ],
                    "extensions": [
                        {
                            "name": "aliases",
                            "value": [
                                "Liplacis",
                                "cis-diamminedichloroplatinum III",
                                "APRD00359",
                                "(sp-4-2)-diamminedichloroplatinum",
                                "cis-Diaminedichloroplatinum",
                                "CDDP",
                                "1,2-Diaminocyclohexaneplatinum II citrate",
                                "Platinol",
                                "Platinol-AQ",
                                "DDP",
                                "cis-platinum",
                                "cisplatino",
                                "PLATINUM, DIAMMINEDICHLORO-, (SP-4-2)-",
                                "Cis-diamminedichloroplatinum ii",
                                "INT230-6 COMPONENT CISPLATIN",
                                "Cis-diamminedichloroplatinum",
                                "DACP",
                                "Cis-DDP",
                                "cisplatinum",
                                "NSC 119875",
                                "cis-diamminedichloroplatinum(II)",
                                "Cis-platin",
                                "(SP-4-2)-DIAMMINEDICHLOROPLATINUM",
                                "NSC119875",
                                "CIS-DIAMMINEDICHLOROPLATINUM",
                                "NSC-119875",
                                "Cisplatinum",
                                "CIS-DIAMMINEDICHLOROPLATINUM II",
                                "INT-230-6 COMPONENT CISPLATIN",
                                "CIS-DDP",
                                "Peyrone's salt",
                                "Cis-platinum ii",
                                "CISplatin",
                                "Fauldiscipla",
                                "CISPLATIN",
                                "Cisplatin",
                                "Mitometh",
                            ],
                        },
                        {
                            "name": "trade_names",
                            "value": [
                                "Platinol",
                                "PLATINOL",
                                "Platinol-aq",
                                "Cisplatin",
                                "PLATINOL-AQ",
                            ],
                        },
                    ],
                },
                "source_meta_": {
                    "RxNorm": {
                        "data_license": "UMLS Metathesaurus",
                        "data_license_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html",
                        "version": "20210104",
                        "data_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html",
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False,
                        },
                    },
                    "NCIt": {
                        "data_license": "CC BY 4.0",
                        "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",
                        "version": "20.09d",
                        "data_url": "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/",
                        "rdp_url": "http://reusabledata.org/ncit.html",
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False,
                        },
                    },
                    "ChemIDplus": {
                        "data_license": "custom",
                        "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",
                        "version": "20200327",
                        "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False,
                        },
                    },
                    "Wikidata": {
                        "data_license": "CC0 1.0",
                        "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
                        "version": "20200812",
                        "data_url": None,
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": False,
                            "share_alike": False,
                        },
                    },
                },
                "service_meta_": {
                    "name": "thera-py",
                    "version": __version__,
                    "response_datetime": "2021-04-05T16:44:15.367831",
                    "url": "https://github.com/cancervariants/therapy-normalization",
                },
            }
        }
    )


class SearchService(BaseModel):
    """Core response schema containing matches for each source"""

    query: str
    warnings: list[dict] = []
    source_matches: dict[SourceName, SourceSearchMatches]
    service_meta_: ServiceMeta

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "query": "cisplatin",
                "warnings": [],
                "source_matches": {
                    "ChemIDplus": {
                        "match_type": 80,
                        "records": [
                            {
                                "concept_id": "chemidplus:15663-27-1",
                                "label": "Cisplatin",
                                "aliases": [
                                    "1,2-Diaminocyclohexaneplatinum II citrate",
                                    "cis-Diaminedichloroplatinum",
                                ],
                                "trade_names": [],
                                "xrefs": ["drugbank:DB00515"],
                                "associated_with": ["unii:Q20Q21Q62J"],
                                "approval_year": [],
                                "has_indication": [],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "custom",
                            "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",
                            "version": "20230222",
                            "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False,
                            },
                        },
                    },
                    "GuideToPHARMACOLOGY": {
                        "match_type": 80,
                        "records": [
                            {
                                "concept_id": "iuphar.ligand:5343",
                                "label": "cisplatin",
                                "aliases": ["Platinol"],
                                "trade_names": [],
                                "xrefs": [
                                    "chembl:CHEMBL11359",
                                    "chemidplus:15663-27-1",
                                    "drugbank:DB00515",
                                ],
                                "associated_with": [
                                    "CHEBI:27899",
                                    "pubchem.compound:441203",
                                    "pubchem.substance:178102005",
                                ],
                                "approval_ratings": ["gtopdb_approved"],
                                "approval_year": [],
                                "has_indication": [],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY-SA 4.0",
                            "data_license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
                            "version": "2023.3",
                            "data_url": "https://www.guidetopharmacology.org/download.jsp",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": True,
                            },
                        },
                    },
                    "Wikidata": {
                        "match_type": 80,
                        "records": [
                            {
                                "concept_id": "wikidata:Q412415",
                                "label": "cisplatin",
                                "aliases": [
                                    "CDDP",
                                    "CIS-DDP",
                                    "Cis-DDP",
                                    "Platinol",
                                    "Platinol-AQ",
                                    "cis-diamminedichloroplatinum(II)",
                                ],
                                "trade_names": [],
                                "xrefs": [
                                    "chembl:CHEMBL11359",
                                    "chemidplus:15663-27-1",
                                    "drugbank:DB00515",
                                    "iuphar.ligand:5343",
                                    "rxcui:2555",
                                ],
                                "associated_with": ["pubchem.compound:5702198"],
                                "approval_year": [],
                                "has_indication": [],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC0 1.0",
                            "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
                            "version": "20231212",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": False,
                                "share_alike": False,
                            },
                        },
                    },
                    "DrugBank": {
                        "match_type": 80,
                        "records": [
                            {
                                "concept_id": "drugbank:DB00515",
                                "label": "Cisplatin",
                                "aliases": [
                                    "APRD00359",
                                    "CDDP",
                                    "Cis-DDP",
                                    "cis-diamminedichloroplatinum(II)",
                                    "cisplatino",
                                ],
                                "trade_names": [],
                                "xrefs": ["chemidplus:15663-27-1"],
                                "associated_with": [
                                    "inchikey:LXZZYRPGZAFOLE-UHFFFAOYSA-L",
                                    "unii:Q20Q21Q62J",
                                ],
                                "approval_year": [],
                                "has_indication": [],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC0 1.0",
                            "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
                            "version": "5.1.10",
                            "data_url": "https://go.drugbank.com/drugs//releases/latest#open-data",
                            "rdp_url": "http://reusabledata.org/drugbank.html",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": False,
                                "share_alike": False,
                            },
                        },
                    },
                    "HemOnc": {
                        "match_type": 80,
                        "records": [
                            {
                                "concept_id": "hemonc:105",
                                "label": "Cisplatin",
                                "aliases": [
                                    "CDDP",
                                    "DACP",
                                    "DDP",
                                    "NSC 119875",
                                    "NSC-119875",
                                    "NSC119875",
                                    "cis-diamminedichloroplatinum III",
                                    "cis-platinum",
                                    "cisplatinum",
                                ],
                                "trade_names": [],
                                "xrefs": ["rxcui:2555"],
                                "associated_with": [],
                                "approval_ratings": ["hemonc_approved"],
                                "approval_year": ["1978"],
                                "has_indication": [
                                    {
                                        "disease_id": "hemonc:569",
                                        "disease_label": "Bladder cancer",
                                        "normalized_disease_id": "ncit:C9334",
                                        "supplemental_info": {"regulatory_body": "FDA"},
                                    },
                                    {
                                        "disease_id": "hemonc:671",
                                        "disease_label": "Testicular cancer",
                                        "normalized_disease_id": "ncit:C7251",
                                        "supplemental_info": {"regulatory_body": "FDA"},
                                    },
                                ],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY 4.0",
                            "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",
                            "version": "2023-09-05",
                            "data_url": "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/9CY9C6",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False,
                            },
                        },
                    },
                    "NCIt": {
                        "match_type": 80,
                        "records": [
                            {
                                "concept_id": "ncit:C376",
                                "label": "Cisplatin",
                                "aliases": [],
                                "trade_names": [],
                                "xrefs": ["chemidplus:15663-27-1"],
                                "associated_with": [
                                    "CHEBI:27899",
                                    "umls:C0008838",
                                    "unii:Q20Q21Q62J",
                                ],
                                "approval_year": [],
                                "has_indication": [],
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY 4.0",
                            "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",
                            "version": "23.10e",
                            "data_url": "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/",
                            "rdp_url": "http://reusabledata.org/ncit.html",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False,
                            },
                        },
                    },
                },
                "service_meta_": {
                    "name": "thera-py",
                    "version": __version__,
                    "response_datetime": "2021-04-05T16:44:15.367831",
                    "url": "https://github.com/cancervariants/therapy-normalization",
                },
            }
        }
    )


class ServiceEnvironment(str, Enum):
    """Define current runtime environment."""

    DEV = "dev"  # local dev
    TEST = "test"  # local/CI testing
    PROD = "prod"  # main production env on cloud
    STAGING = "staging"  # staging env on cloud


LAB_EMAIL = "Alex.Wagner@nationwidechildrens.org"
LAB_WEBPAGE_URL = "https://www.nationwidechildrens.org/specialties/institute-for-genomic-medicine/research-labs/wagner-lab"
DOCS_URL = "https://github.com/cancervariants/therapy-normalization"
APP_DESCRIPTION = "Resolve ambiguous references and descriptions of drugs and therapies to consistently-structured, normalized terms"


class ServiceType(BaseModel):
    """Define service_info response for type field"""

    group: Literal["org.genomicmedlab"] = "org.genomicmedlab"
    artifact: Literal["Thera-Py API"] = "Thera-Py API"
    version: Literal[__version__] = __version__


class ServiceOrganization(BaseModel):
    """Define service_info response for organization field"""

    name: Literal["Genomic Medicine Lab at Nationwide Children's Hospital"] = (
        "Genomic Medicine Lab at Nationwide Children's Hospital"
    )
    url: Literal[LAB_WEBPAGE_URL] = LAB_WEBPAGE_URL


class ServiceInfo(BaseModel):
    """Define response structure for GA4GH /service_info endpoint."""

    id: Literal["org.genomicmedlab.thera_py"] = "org.genomicmedlab.thera_py"
    name: Literal["thera_py"] = "thera_py"
    type: ServiceType
    description: Literal[APP_DESCRIPTION] = APP_DESCRIPTION
    organization: ServiceOrganization
    contactUrl: Literal[LAB_EMAIL] = LAB_EMAIL  # noqa: N815
    documentationUrl: Literal[DOCS_URL] = DOCS_URL  # noqa: N815
    createdAt: Literal["2021-01-01T00:00:00+00:00"] = "2021-01-01T00:00:00+00:00"  # noqa: N815
    updatedAt: str | None = None  # noqa: N815
    environment: ServiceEnvironment
    version: Literal[__version__] = __version__
