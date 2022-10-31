"""This module contains data models for representing VICC therapy records."""
from typing import List, Literal, Optional, Dict, Union, Any, Type, Set
from enum import Enum, IntEnum
from datetime import datetime

from ga4gh.vrsatile.pydantic import return_value
from ga4gh.vrsatile.pydantic.vrs_models import CURIE
from ga4gh.vrsatile.pydantic.vrsatile_models import ValueObjectDescriptor
from pydantic import BaseModel, StrictBool, validator

from therapy.version import __version__

# Working structure for object in preparation for upload to DB
RecordParams = Dict[str, Union[List, Set, str, Dict[str, Any]]]


class ApprovalRating(str, Enum):
    """Define string constraints for approval rating attribute. This reflects a drug's
    regulatory approval status as evaluated by an individual source. We opted to retain
    each individual source's rating as a distinct enum value after finding that some
    sources disagreed on the true value (either because of differences in scope like
    US vs EU/other regulatory arenas, old or conflicting data, or other reasons). Value
    descriptions are provided below from each listed source.

    ChEMBL:
     - CHEMBL_PHASE_0: "Research: The compound has not yet reached clinical trials
    (preclinical/research compound)"
     - CHEMBL_PHASE_1: "The compound has reached Phase I clinical trials (safety
    studies, usually with healthy volunteers)"
     - CHEMBL_PHASE_2: "The compound has reached Phase II clinical trials (preliminary
    studies of effectiveness)"
     - CHEMBL_PHASE_3: "The compound has reached Phase III clinical trials (larger
    studies of safety and effectiveness)"
     - CHEMBL_PHASE_4: The compound has been approved in at least one country or area."
     - CHEMBL_WITHDRAWN: "A withdrawn drug is an approved drug contained in a medicinal
    product that subsequently had been removed from the market. The reasons for
    withdrawal may include toxicity, lack of efficacy, or other reasons such as an
    unfavorable risk-to-benefit ratio following approval and marketing of the drug.
    ChEMBL considers an approved drug to be withdrawn only if all medicinal products
    that contain the drug as an active ingredient have been withdrawn from one (or more)
    regions of the world. Note that all medicinal products for a drug can be withdrawn
    in one region of the world while still being marketed in other jurisdictions."
    https://pubs.acs.org/doi/10.1021/acs.chemrestox.0c00296

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

    CHEMBL_0 = "chembl_phase_0"
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
    normalized_disease_id: Optional[str]
    supplemental_info: Optional[Dict[str, str]] = None

    class Config:
        """Configure HasIndication class"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["HasIndication"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = [
                {
                    "disease_id": "mesh:D016778",
                    "disease_label": "Malaria, Falciparum",
                    "normalized_disease_id": "ncit:C34798",
                    "supplemental_info": {
                        "chembl_max_phase_for_ind": "chembl_phase_2"
                    }
                },
                {
                    "disease_id": "hemonc:634",
                    "disease_label": "Myelodysplastic syndrome",
                    "normalized_disease_id": "ncit:C3247",
                    "supplemental_info": {
                        "regulatory_body": "FDA"
                    }
                }
            ]


class Drug(BaseModel):
    """A pharmacologic substance used to treat a medical condition."""

    concept_id: str
    label: Optional[str] = None
    aliases: Optional[List[str]] = []
    trade_names: Optional[List[str]] = []
    xrefs: Optional[List[str]] = []
    associated_with: Optional[List[str]] = []
    approval_ratings: Optional[List[ApprovalRating]] = None
    approval_year: Optional[List[str]] = []
    has_indication: Optional[List[HasIndication]] = []

    class Config:
        """Configure Drug class"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["Drug"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
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
                    "Platinol-Aq"
                ],
                "xrefs": [],
                "associated_with": None,
                "approval_ratings": "approved",
                "approval_year": [],
                "has_indication": [],
                "trade_names": ["PLATINOL", "PLATINOL-AQ", "CISPLATIN"]
            }


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
    DRUGSATFDA_ANDA = DRUGSATFDA
    GUIDETOPHARMACOLOGY = "GuideToPHARMACOLOGY"


class NamespacePrefix(Enum):
    """Define string constraints for namespace prefixes on concept IDs."""

    ATC = "atc"  # Anatomical Therapeutic Chemical Classification System
    BINDINGDB = "bindingdb"
    CHEBI = "CHEBI"
    CHEMBL = "chembl"
    CHEMIDPLUS = "chemidplus"
    CASREGISTRY = CHEMIDPLUS
    CHEMSPIDER = "chemspider"
    CVX = "cvx"  # Vaccines Administered
    DRUGBANK = "drugbank"
    DRUGCENTRAL = "drugcentral"
    DRUGSATFDA_ANDA = "drugsatfda.anda"
    DRUGSATFDA_NDA = "drugsatfda.nda"
    HEMONC = "hemonc"
    INCHIKEY = "inchikey"
    ISO = "iso"
    IUPHAR = "iuphar"
    IUPHAR_LIGAND = "iuphar.ligand"
    GUIDETOPHARMACOLOGY = IUPHAR_LIGAND
    KEGGCOMPOUND = "kegg.compound"
    KEGGDRUG = "kegg.drug"
    MMSL = "mmsl"  # Multum MediSource Lexicon
    MSH = "mesh"  # Medical Subject Headings
    MTHCMSFRF = "mthcmsfrf"  # CMS Formulary Reference File
    NCIT = "ncit"
    NDC = "ndc"  # National Drug Code
    PDB = "pdb"
    PHARMGKB = "pharmgkb.drug"
    PUBCHEMCOMPOUND = "pubchem.compound"
    PUBCHEMSUBSTANCE = "pubchem.substance"
    RXNORM = "rxcui"
    SPL = "spl"  # Structured Product Labeling
    THERAPEUTICTARGETSDB = "ttd"
    UMLS = "umls"
    UNII = "unii"
    UNIPROT = "uniprot"
    USP = "usp"  # USP Compendial Nomenclature
    VANDF = "vandf"  # Veterans Health Administration National Drug File
    WIKIDATA = "wikidata"
    ZINC = "zinc"


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
    data_url: Optional[str]
    rdp_url: Optional[str]
    data_license_attributes: Dict[str, StrictBool]

    class Config:
        """Configure OpenAPI schema"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["SourceMeta"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
                "data_license": "CC BY-SA 3.0",
                "data_license_url":
                    "https://creativecommons.org/licenses/by-sa/3.0/",
                "version": "27",
                "data_url":
                    "http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/",  # noqa: E501
                "rdp_url": "http://reusabledata.org/chembl.html",
                "data_license_attributes": {
                    "non_commercial": False,
                    "share_alike": True,
                    "attribution": True
                }
            }


class MatchesKeyed(BaseModel):
    """Container for matching information from an individual source.
    Used when matches are requested as an object, not an array.
    """

    match_type: MatchType
    records: List[Drug]
    source_meta_: SourceMeta

    class Config:
        """Configure OpenAPI schema"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["MatchesKeyed"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
                "match_type": 0,
                "records": [],
                "source_meta_": {
                    "data_license": "CC BY-SA 3.0",
                    "data_license_url":
                        "https://creativecommons.org/licenses/by-sa/3.0/",
                    "version": "27",
                    "data_url":
                        "http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/",  # noqa: E501
                    "rdp_url": "http://reusabledata.org/chembl.html",
                    "data_license_attributes": {
                        "non_commercial": False,
                        "share_alike": True,
                        "attribution": True
                    }
                },
            }


class MatchesListed(BaseModel):
    """Container for matching information from an individual source.
    Used when matches are requested as an array, not an object.
    """

    source: SourceName
    match_type: MatchType
    records: List[Drug]
    source_meta_: SourceMeta

    class Config:
        """Configure openAPI schema"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["MatchesListed"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
                "source": "ChEMBL",
                "match_type": 0,
                "records": [],
                "source_meta_": {
                    "data_license": "CC BY-SA 3.0",
                    "data_license_url":
                        "https://creativecommons.org/licenses/by-sa/3.0/",
                    "version": "27",
                    "data_url":
                        "http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_27/",  # noqa: E501
                    "rdp_url": "http://reusabledata.org/chembl.html",
                    "data_license_attributes": {
                        "non_commercial": False,
                        "share_alike": True,
                        "attribution": True
                    }
                },
            }


class ApprovalRatingValue(BaseModel):
    """VOD Extension class for regulatory approval rating/indication
    value attributes.
    """

    approval_ratings: Optional[List[ApprovalRating]]
    approval_year: Optional[List[str]]
    has_indication: Optional[List[ValueObjectDescriptor]]


class TherapyDescriptor(ValueObjectDescriptor):
    """Create therapy descriptor model"""

    type: Literal["TherapyDescriptor"] = "TherapyDescriptor"
    therapy_id: CURIE

    _get_therapy_id_val = validator("therapy_id", allow_reuse=True)(return_value)


class ServiceMeta(BaseModel):
    """Metadata regarding the therapy-normalization service."""

    name = "thera-py"
    version = __version__
    response_datetime: datetime
    url = "https://github.com/cancervariants/therapy-normalization"

    class Config:
        """Configure OpenAPI schema"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["ServiceMeta"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
                "name": "thera-py",
                "version": "0.1.0",
                "response_datetime": "2021-04-05T16:44:15.367831",
                "url": "https://github.com/cancervariants/therapy-normalization"
            }


class MatchesNormalized(BaseModel):
    """Matches associated with normalized concept from a single source."""

    records: List[Drug]
    source_meta_: SourceMeta

    class Config:
        """Configure OpenAPI schema"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type["MatchesNormalized"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)


class BaseNormalizationService(BaseModel):
    """Base method providing shared attributes to Normalization service classes."""

    query: str
    warnings: Optional[List[Dict]]
    match_type: MatchType
    service_meta_: ServiceMeta


class UnmergedNormalizationService(BaseNormalizationService):
    """Response providing source records corresponding to normalization of user query.
    Enables retrieval of normalized concept while retaining sourcing for accompanying
    attributes.
    """

    normalized_concept_id: Optional[CURIE]
    source_matches: Dict[SourceName, MatchesNormalized]

    class Config:
        """Configure OpenAPI schema"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type["UnmergedNormalizationService"]) -> None:
            """Configure OpenAPI schema example"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
                "query": "L745870",
                "warnings": [],
                "match_type": 80,
                "service_meta_": {
                    "response_datetime": "2022-04-22T11:40:18.921859",
                    "name": "thera-py",
                    "version": "0.3.4",
                    "url": "https://github.com/cancervariants/therapy-normalization"
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
                                    "3-[[4-(4-chlorophenyl)piperazin-1-yl]methyl]-1H-pyrrolo[2,3-b]pyridine"  # noqa: E501
                                ],
                                "trade_names": [],
                                "xrefs": [
                                    "chemidplus:158985-00-3",
                                    "chembl:CHEMBL267014"
                                ],
                                "associated_with": [
                                    "pubchem.substance:178100340",
                                    "pubchem.compound:5311200",
                                    "inchikey:OGJGQVFWEPNYSB-UHFFFAOYSA-N"
                                ],
                                "approval_ratings": None,
                                "approval_year": [],
                                "has_indication": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY-SA 4.0",
                            "data_license_url": "https://creativecommons.org/licenses/by-sa/4.0/",  # noqa: E501
                            "version": "2021.4",
                            "data_url": "https://www.guidetopharmacology.org/download.jsp",  # noqa: E501
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "share_alike": True,
                                "attribution": True
                            }
                        }
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
                                "approval_ratings": [
                                    "chembl_phase_0"
                                ],
                                "approval_year": [],
                                "has_indication": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY-SA 3.0",
                            "data_license_url": "https://creativecommons.org/licenses/by-sa/3.0/",  # noqa: E501
                            "version": "29",
                            "data_url": "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_29",  # noqa: E501
                            "rdp_url": "http://reusabledata.org/chembl.html",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "share_alike": True,
                                "attribution": True
                            }
                        }
                    }
                }
            }


class NormalizationService(BaseNormalizationService):
    """Response containing one or more merged records and source data."""

    therapy_descriptor: Optional[TherapyDescriptor]
    source_meta_: Optional[Dict[SourceName, SourceMeta]]

    class Config:
        """Configure OpenAPI schema"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any],
                         model: Type["NormalizationService"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
                "query": "cisplatin",
                "warnings": None,
                "match_type": 80,
                "therapy_descriptor": {
                    "id": "normalize.therapy:cisplatin",
                    "type": "TherapyDescriptor",
                    "therapy_id": "rxcui:2555",
                    "label": "cisplatin",
                    "xrefs": [
                        "ncit:C376", "chemidplus:15663-27-1",
                        "wikidata:Q412415"
                    ],
                    "alternate_labels": [
                        "CIS-DDP", "cis Platinum", "DDP",
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
                        "Platinol-AQ", "Platinol",
                        "Platinum Diamminodichloride"
                    ],
                    "extensions": [
                        {
                            "type": "Extension",
                            "name": "trade_names",
                            "value": [
                                "Platinol", "Cisplatin"
                            ]
                        },
                        {
                            "type": "Extension",
                            "name": "associated_with",
                            "value": [
                                "atc:L01XA01",
                                "mmsl:4456",
                                "chebi:CHEBI:27899",
                                "pubchem.compound:5702198",
                                "umls:C0008838",
                                "usp:m17910",
                                "fda:Q20Q21Q62J",
                                "mmsl:d00195",
                                "mthspl:Q20Q21Q62J",
                                "mmsl:31747",
                                "mesh:D002945",
                                "vandf:4018139"
                            ]
                        }
                    ]
                },
                "source_meta_": {
                    "RxNorm": {
                        "data_license": "UMLS Metathesaurus",
                        "data_license_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html",  # noqa: E501
                        "version": "20210104",
                        "data_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html",  # noqa: E501
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False
                        }
                    },
                    "NCIt": {
                        "data_license": "CC BY 4.0",
                        "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa: E501
                        "version": "20.09d",
                        "data_url": "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/20.09d_Release/",  # noqa: E501
                        "rdp_url": "http://reusabledata.org/ncit.html",
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False
                        }
                    },
                    "ChemIDplus": {
                        "data_license": "custom",
                        "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                        "version": "20200327", "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",  # noqa: E501
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": True,
                            "share_alike": False
                        }
                    },
                    "Wikidata": {
                        "data_license": "CC0 1.0",
                        "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",  # noqa: E501
                        "version": "20200812",
                        "data_url": None,
                        "rdp_url": None,
                        "data_license_attributes": {
                            "non_commercial": False,
                            "attribution": False,
                            "share_alike": False
                        }
                    }
                },
                "service_meta_": {
                    "name": "thera-py",
                    "version": "0.1.0",
                    "response_datetime": "2021-04-05T16:44:15.367831",
                    "url": "https://github.com/cancervariants/therapy-normalization"
                }
            }


class SearchService(BaseModel):
    """Core response schema containing matches for each source"""

    query: str
    warnings: Optional[List[Dict]]
    source_matches: Union[Dict[SourceName, MatchesKeyed], List[MatchesListed]]
    service_meta_: ServiceMeta

    class Config:
        """Enables orm_mode"""

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type["SearchService"]) -> None:
            """Configure OpenAPI schema"""
            if "title" in schema.keys():
                schema.pop("title", None)
            for prop in schema.get("properties", {}).values():
                prop.pop("title", None)
            schema["example"] = {
                "query": "cisplatin",
                "warnings": None,
                "source_matches": [
                    {
                        "source": "ChemIDplus",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "Cisplatin",
                                "concept_id": "chemidplus:15663-27-1",
                                "aliases": [
                                    "cis-Diaminedichloroplatinum",
                                    "1,2-Diaminocyclohexaneplatinum II citrate"
                                ],
                                "xrefs": ["drugbank:DB00515"],
                                "associated_with": ["fda:Q20Q21Q62J"],
                                "approval_ratings": None,
                                "trade_names": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "custom",
                            "data_license_url": "https://www.nlm.nih.gov/databases/download/terms_and_conditions.html",  # noqa: E501
                            "version": "20210204",
                            "data_url": "ftp://ftp.nlm.nih.gov/nlmdata/.chemidlease/",
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False
                            }
                        }
                    },
                    {
                        "source": "RxNorm",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "cisplatin",
                                "concept_id": "rxcui:2555",
                                "aliases": [
                                    "cis-Dichlorodiammineplatinum(II)",
                                    "Platinum Diamminodichloride",
                                    "cis Diamminedichloroplatinum",
                                    "cis-diamminedichloroplatinum(II)",
                                    "cis-Diamminedichloroplatinum",
                                    "cis Platinum",
                                    "CDDP",
                                    "Dichlorodiammineplatinum",
                                    "cis-Platinum",
                                    "CISplatin",
                                    "cis-Diamminedichloroplatinum(II)",
                                    "Cis-DDP",
                                    "DDP",
                                    "Diamminodichloride, Platinum"
                                ],
                                "xrefs": [
                                    "drugbank:DB00515",
                                    "drugbank:DB12117"
                                ],
                                "associated_with": [
                                    "usp:m17910",
                                    "vandf:4018139",
                                    "mesh:D002945",
                                    "mthspl:Q20Q21Q62J",
                                    "mmsl:d00195",
                                    "atc:L01XA01",
                                    "mmsl:31747",
                                    "mmsl:4456"
                                ],
                                "approval_ratings": ["rxnorm_prescribable"],
                                "trade_names": [
                                    "Cisplatin",
                                    "Platinol"
                                ]
                            }
                        ],
                        "source_meta_": {
                            "data_license": "UMLS Metathesaurus",
                            "data_license_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html",  # noqa: E501
                            "version": "20210104",
                            "data_url": "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html",  # noqa: E501
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False
                            }
                        }
                    },
                    {
                        "source": "NCIt",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "Cisplatin",
                                "concept_id": "ncit:C376",
                                "aliases": [],
                                "xrefs": ["chemidplus:15663-27-1"],
                                "associated_with": [
                                    "umls:C0008838",
                                    "fda:Q20Q21Q62J",
                                    "chebi:CHEBI:27899"
                                ],
                                "approval_ratings": None,
                                "trade_names": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC BY 4.0",
                            "data_license_url": "https://creativecommons.org/licenses/by/4.0/legalcode",  # noqa: E501
                            "version": "20.09d",
                            "data_url": "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/2020/20.09d_Release/",  # noqa: E501
                            "rdp_url": "http://reusabledata.org/ncit.html",
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": True,
                                "share_alike": False
                            }
                        }
                    },
                    {
                        "source": "Wikidata",
                        "match_type": 80,
                        "records": [
                            {
                                "label": "cisplatin",
                                "concept_id": "wikidata:Q412415",
                                "aliases": [
                                    "Platinol",
                                    "cis-diamminedichloroplatinum(II)",
                                    "CDDP",
                                    "Cis-DDP",
                                    "CIS-DDP",
                                    "Platinol-AQ"
                                ],
                                "xrefs": [
                                    "chemidplus:15663-27-1",
                                    "chembl:CHEMBL11359",
                                    "rxcui:2555",
                                    "drugbank:DB00515"
                                ],
                                "associated_with": [
                                    "pubchem.compound:5702198"
                                ],
                                "approval_ratings": None,
                                "trade_names": []
                            }
                        ],
                        "source_meta_": {
                            "data_license": "CC0 1.0",
                            "data_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",  # noqa: E501
                            "version": "20210331",
                            "data_url": None,
                            "rdp_url": None,
                            "data_license_attributes": {
                                "non_commercial": False,
                                "attribution": False,
                                "share_alike": False
                            }
                        }
                    }
                ],
                "service_meta_": {
                    "name": "thera-py",
                    "version": "0.1.0",
                    "response_datetime": "2021-04-05T16:44:15.367831",
                    "url": "https://github.com/cancervariants/therapy-normalization"
                }
            }
