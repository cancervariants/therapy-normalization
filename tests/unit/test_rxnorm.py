"""Test that the therapy normalizer works as intended for the RxNorm source."""

import isodate
import pytest

from therapy.etl import RxNorm
from therapy.schemas import MatchType, Therapy


@pytest.fixture(scope="module")
def rxnorm(test_source):
    """Provide test chembl query endpoint"""
    return test_source(RxNorm)


@pytest.fixture(scope="module")
def bifidobacterium_infantis():
    """Create bifidobacterium infantis drug fixture."""
    params = {
        "label": "Bifidobacterium infantis",
        "concept_id": "rxcui:100213",
        "aliases": [
            "Bifidobacterium infantis whole",
            "Bifidobacterium lactentis whole",
            "Bifidobacterium liberorum whole",
            "Bifidobacterium longum subsp. infantis",
            "Bifidobacterium longum subsp. infantis whole",
            "Bifidobacterium longum infantis whole",
            "bifidobacterium infantis",
            "Bifidobacterium longum bv. infantis whole",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": ["drugbank:DB14222"],
        "associated_with": ["mmsl:d07347", "unii:EF3U3H7I76", "mmsl:32421"],
        "trade_names": ["Align", "Evivo"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def cisplatin():
    """Create cisplatin drug fixture."""
    params = {
        "label": "cisplatin",
        "concept_id": "rxcui:2555",
        "aliases": [
            "(SP-4-2)-DIAMMINEDICHLOROPLATINUM",
            "CDDP",
            "CIS-DIAMMINEDICHLOROPLATINUM",
            "CIS-DIAMMINEDICHLOROPLATINUM II",
            "CISplatin",
            "Cis-DDP",
            "DDP",
            "PLATINUM, DIAMMINEDICHLORO-, (SP-4-2)-",
            "cis-diamminedichloroplatinum(II)",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": ["drugbank:DB00515", "drugbank:DB12117"],
        "associated_with": [
            "usp:m17910",
            "vandf:4018139",
            "unii:Q20Q21Q62J",
            "mmsl:d00195",
            "atc:L01XA01",
            "mmsl:31747",
            "mmsl:4456",
        ],
        "trade_names": ["Cisplatin", "Platinol"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def amiloride_hydrochloride():
    """Create amiloride hydrochloride drug fixture."""
    params = {
        "label": "amiloride hydrochloride",
        "concept_id": "rxcui:142424",
        "aliases": [
            "aMILoride hydrochloride",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": [],
        "associated_with": [
            "usp:m2650",
            "unii:FZJ37245UC",
            "mmsl:2658",
            "mmsl:4166",
            "vandf:4017935",
        ],
        "trade_names": [
            "AMILoride Hydrochloride",
            "Midamor",
            "Aridil",
            "Frusemek",
            "Moduret",
            "Moduretic",
            "Zida-Co",
        ],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def amiloride():
    """Create amiloride drug fixture."""
    params = {
        "label": "amiloride",
        "concept_id": "rxcui:644",
        "aliases": [
            "3,5-diamino-N-carbamimidoyl-6-chloropyrazine-2-carboxamide",
            "aMILoride",
            "Amipramidin",
            "Amipramidine",
            "N-amidino-3,5-diamino-6-chloropyrazinecarboxamide",
            "Amyloride",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": ["drugbank:DB00594"],
        "associated_with": [
            "vandf:4019603",
            "unii:7DZO8EB0Z3",
            "mmsl:d00169",
            "atc:C03DB01",
        ],
        "trade_names": [
            "Amilamont",
        ],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def timolol():
    """Create timolol drug fixture."""
    params = {
        "label": "timolol",
        "concept_id": "rxcui:10600",
        "aliases": [
            "(S)-1-(tert-butylamino)-3-[(4-morpholin-4-yl-1,2,5-thiadiazol-3-yl)oxy]propan-2-ol",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": ["drugbank:DB00373"],
        "associated_with": [
            "vandf:4019949",
            "unii:817W3C6175",
            "mmsl:d00139",
            "atc:C07AA06",
            "atc:S01ED01",
        ],
        "trade_names": [
            "Betimol",
            "Timoptic",
            "Istalol",
            "Combigan",
            "Cosopt",
            "Timoptol LA",
            "Blocadren",
            "Timolide",
            "Betim",
            "Glau-Opt",
            "Glaucol",
            "Timoptol",
        ],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def lymphocyte():
    """Create lymphocyte immune globulin, anti-thymocyte globulin drug
    fixture.
    """
    params = {
        "label": "lymphocyte immune globulin, anti-thymocyte globulin",
        "concept_id": "rxcui:1011",
        "aliases": [
            "lymphocyte immune globulin, anti-thy (obs)",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": [],
        "associated_with": [
            "vandf:4022194",
            "vandf:4018097",
            "mmsl:d01141",
            "mmsl:5001",
        ],
        "trade_names": ["ATGAM", "Thymoglobulin"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def aspirin():
    """Create aspirin drug fixture."""
    params = {
        "label": "aspirin",
        "concept_id": "rxcui:1191",
        "aliases": [
            "Salicylic acid acetate",
            "Aspirin",
            "2-Acetoxybenzenecarboxylic acid",
            "ASA",
            "O-acetylsalicylic acid",
            "2-Acetoxybenzoic acid",
            "o-acetoxybenzoic acid",
            "o-carboxyphenyl acetate",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": ["drugbank:DB00945"],
        "associated_with": [
            "usp:m6240",
            "vandf:4017536",
            "mmsl:34512",
            "unii:R16CO5Y76E",
            "mmsl:244",
            "mmsl:4223",
            "mmsl:d00170",
            "atc:A01AD05",
            "atc:B01AC06",
            "atc:N02BA01",
        ],
        "trade_names": [],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def mesna():
    """Create mesna drug fixture."""
    params = {
        "label": "mesna",
        "concept_id": "rxcui:44",
        "aliases": [],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": [],
        "associated_with": [
            "usp:m49500",
            "vandf:4019477",
            "unii:NR7O1405Q9",
            "atc:R05CB05",
            "atc:V03AF01",
            "mmsl:41498",
            "mmsl:5057",
            "mmsl:d01411",
        ],
        "trade_names": ["Mesna", "Mesnex", "Mesna Nova Plus"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def beta_alanine():
    """Create beta-alanine drug fixture."""
    params = {
        "label": "beta-alanine",
        "concept_id": "rxcui:61",
        "aliases": [],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": [],
        "associated_with": ["vandf:4028377", "unii:11P2JDE17B"],
        "trade_names": [],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def algestone():
    """Create algestone drug fixture."""
    params = {
        "label": "algestone",
        "concept_id": "rxcui:595",
        "aliases": [],
        "approval_ratings": [],
        "xrefs": [],
        "associated_with": [],
        "trade_names": [],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def levothyroxine():
    """Create levothyroxine drug fixture."""
    params = {
        "label": "levothyroxine",
        "concept_id": "rxcui:10582",
        "aliases": [
            "3,5,3',5'-Tetraiodo-L-thyronine",
            "Thyroxine",
            "O-(4-Hydroxy-3,5-diiodophenyl)-3,5-diiodo-L-tyrosine",
            "L-T4",
            "LT4",
            "T4",
            "3,3',5,5'-Tetraiodo-L-thyronine",
            "L-Thyroxine",
            "O-(4-Hydroxy-3,5-diidophenyl)-3,5-diiodo-L-tyrosine",
            "Levothyroxin",
            "4-(4-Hydroxy-3,5-diiodophenoxy)-3,5-diiodo-L-phenylalanine",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": ["drugbank:DB00451"],
        "associated_with": [
            "vandf:4022126",
            "mmsl:d00278",
            "unii:Q51BO43MG4",
        ],
        # trade names length > 20
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def fluoxetine():
    """Create fluoxetine drug fixture."""
    params = {
        "label": "fluoxetine",
        "concept_id": "rxcui:4493",
        "aliases": [
            "FLUoxetine",
            "(+-)-N-Methyl-gamma-(4-(trifluoromethyl)phenoxy)" "benzenepropanamine",
            "(+-)-N-Methyl-3-phenyl-3-((alpha,alpha,alpha-trifluoro-"
            "P-tolyl)oxy)propylamine",
        ],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": ["drugbank:DB00472"],
        "associated_with": [
            "mmsl:17711",
            "vandf:4019761",
            "mmsl:d00236",
            "atc:N06AB03",
            "unii:01K63SUP8D",
        ],
        "trade_names": [
            "Prozac",
            "Sarafem",
            "Symbyax",
            "Rapiflux",
            "Selfemra",
        ],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def fluoxetine_hydrochloride():
    """Create fluoxetine hydrochloride drug fixture."""
    params = {
        "label": "fluoxetine hydrochloride",
        "concept_id": "rxcui:227224",
        "aliases": ["FLUoxetine hydrochloride"],
        "approval_ratings": ["rxnorm_prescribable"],
        "xrefs": [],
        "associated_with": [
            "usp:m33780",
            "unii:I9W7N6B1KJ",
            "mmsl:28237",
            "mmsl:4746",
            "vandf:4019389",
            "mmsl:41730",
            "mmsl:37675",
        ],
        "trade_names": ["FLUoxetine HCl", "FLUoxetine Hydrochloride", "RECONCILE"],
    }
    return Therapy(**params)


@pytest.fixture(scope="module")
def phenobarbital():
    """Create phenobarbital fixture."""
    return Therapy(
        concept_id="rxcui:8134",
        label="phenobarbital",
        aliases=[
            "Phenobarbituric Acid",
            "Phenylethylbarbiturate",
            "5-ethyl-5-phenylpyrimidine-2,4,6(1H,3H,5H)-trione",
            "5-Phenyl-5-ethylbarbituric acid",
            "Phenylethylbarbituric Acid",
            "5-Ethyl-5-phenylbarbituric acid",
            "Phenobarbitone",
            "Phenobarbitol",
            "5-ethyl-5-phenyl-2,4,6(1H,3H,5H)-pyrimidinetrione",
            "PHENobarbital",
            "5-Ethyl-5-phenyl-pyrimidine-2,4,6-trione",
            "Phenylethylmalonylurea",
        ],
        xrefs=["drugbank:DB01174"],
        associated_with=[
            "mmsl:d00340",
            "usp:m63400",
            "mmsl:2390",
            "mmsl:5272",
            "vandf:4017422",
            "unii:YQE403BP4D",
            "atc:N03AA02",
        ],
        approval_ratings=["rxnorm_prescribable"],
        approval_year=[],
        has_indication=[],
    )


def test_bifidobacterium_infantis(bifidobacterium_infantis, rxnorm, compare_records):
    """Test that bifidobacterium_ nfantis drug normalizes to
    correct drug concept.
    """
    # Concept ID Match
    response = rxnorm.search("RxCUI:100213")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], bifidobacterium_infantis)

    # Label Match
    response = rxnorm.search(" Bifidobacterium infantis")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], bifidobacterium_infantis)

    response = rxnorm.search("bifidobacterium infantis")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], bifidobacterium_infantis)

    # Trade Name Match
    response = rxnorm.search("ALIGN")
    assert response.match_type == MatchType.TRADE_NAME
    assert len(response.records) == 1
    compare_records(response.records[0], bifidobacterium_infantis)

    response = rxnorm.search("evivo")
    assert response.match_type == MatchType.TRADE_NAME
    assert len(response.records) == 1
    compare_records(response.records[0], bifidobacterium_infantis)

    # Xref Match
    response = rxnorm.search("drugbank:DB14222")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], bifidobacterium_infantis)


def test_cisplatin(cisplatin, rxnorm, compare_records):
    """Test that cisplatin drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxCUI:2555")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    # Label Match
    response = rxnorm.search("CISPLATIN")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    # Alias Match
    response = rxnorm.search("Cis-DDP")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    # Trade Name Match
    response = rxnorm.search("platinol")
    assert response.match_type == MatchType.TRADE_NAME
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)

    # Xref Match
    response = rxnorm.search("drugbank:DB12117")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], cisplatin)


def test_amiloride_hydrochloride(amiloride_hydrochloride, rxnorm, compare_records):
    """Test that amiloride_hydrochloride hydrochloride, anhydrous drug
    normalizes to correct drug concept.
    """
    # Concept ID Match
    response = rxnorm.search("RxcUI:142424")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride_hydrochloride)

    # Label Match
    response = rxnorm.search("amiloride hydrochloride")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride_hydrochloride)

    # Trade Name Match
    response = rxnorm.search("Midamor")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride_hydrochloride)


def test_amiloride(amiloride, rxnorm, compare_records):
    """Test that amiloride hydrochloride, anhydrous drug normalizes to
    correct drug concept.
    """
    # Concept ID Match
    response = rxnorm.search("RxcUI:644")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride)

    # Label Match
    response = rxnorm.search("amiloride")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride)

    # Alias Match
    response = rxnorm.search("Amyloride")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride)

    response = rxnorm.search(
        "3,5-diamino-N-carbamimidoyl-6-" "chloropyrazine-2-carboxamide"
    )
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride)


def test_timolol(timolol, rxnorm, compare_records):
    """Test that timolol drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxcUI:10600")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], timolol)

    # Label Match
    response = rxnorm.search("timolol")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], timolol)

    # Alias Match
    response = rxnorm.search(
        "(S)-1-(tert-butylamino)-3-[(4-"
        "morpholin-4-yl-1,2,5-thiadiazol"
        "-3-yl)oxy]propan-2-ol"
    )
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], timolol)

    # Trade Name Match
    response = rxnorm.search("Betimol")
    assert response.match_type == MatchType.TRADE_NAME
    assert len(response.records) == 1
    compare_records(response.records[0], timolol)


def test_lymphocyte(lymphocyte, rxnorm, compare_records):
    """Test that lymphocyte drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxCUI:1011")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], lymphocyte)

    # Label Match
    response = rxnorm.search("lymphocyte immune globulin, " "anti-thymocyte globulin")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], lymphocyte)

    # Trade Name Match
    response = rxnorm.search("Thymoglobulin")
    assert response.match_type == MatchType.TRADE_NAME
    assert len(response.records) == 2
    response.records.sort(key=lambda r: r.concept_id)
    compare_records(response.records[0], lymphocyte)

    response = rxnorm.search("ATGAM")
    assert response.match_type == MatchType.TRADE_NAME
    assert len(response.records) == 2
    response.records.sort(key=lambda r: r.concept_id)
    compare_records(response.records[0], lymphocyte)


def test_aspirin(aspirin, rxnorm, compare_records):
    """Test that aspirin drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxcUI:1191")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], aspirin)

    # (Trade Name) No Match
    response = rxnorm.search("Anacin")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0


def test_mesnan(mesna, rxnorm, compare_records):
    """Test that mesna drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxCUI:44")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], mesna)

    # Label Match
    response = rxnorm.search("mesna")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], mesna)

    # Trade Name Match
    response = rxnorm.search("Mesnex")
    assert response.match_type == MatchType.TRADE_NAME
    assert len(response.records) == 1
    compare_records(response.records[0], mesna)


def test_beta_alanine(beta_alanine, rxnorm, compare_records):
    """Test that beta_alanine drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxCUI:61")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], beta_alanine)

    # Label Match
    response = rxnorm.search("beta-alanine")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], beta_alanine)


def test_algestone(algestone, rxnorm, compare_records):
    """Test that algestone drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxCUI:595")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], algestone)

    # Label Match
    response = rxnorm.search("algestone")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], algestone)


def test_levothyroxine(levothyroxine, rxnorm, compare_records):
    """Test that levothyroxine drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxCUI:10582")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], levothyroxine)

    # Label Match
    response = rxnorm.search("levothyroxine")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], levothyroxine)

    # Alias Match
    response = rxnorm.search("LT4")
    assert response.match_type == MatchType.ALIAS
    assert len(response.records) == 1
    compare_records(response.records[0], levothyroxine)

    # trade name matches fail because they exceed the max # limit
    response = rxnorm.search("Unithroid")
    assert response.match_type == MatchType.NO_MATCH

    response = rxnorm.search("Euthyrox")
    assert response.match_type == MatchType.NO_MATCH

    # Xref Match
    response = rxnorm.search("DRUGBANK:DB00451")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], levothyroxine)


def test_fluoxetine(fluoxetine, rxnorm, compare_records):
    """Test that fluoxetine drug normalizes to correct drug concept."""
    # Concept ID Match
    response = rxnorm.search("RxCUI:4493")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], fluoxetine)

    # Label Match
    response = rxnorm.search("fluoxetine")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], fluoxetine)


def test_fluoxetine_hydrochloride(fluoxetine_hydrochloride, rxnorm, compare_records):
    """Test that fluoxetine_hydrochloride drug normalizes to correct drug
    concept.
    """
    # Concept ID Match
    response = rxnorm.search("RxCUI:227224")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], fluoxetine_hydrochloride)

    # Label Match
    response = rxnorm.search("fluoxetine hydrochloride")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], fluoxetine_hydrochloride)


def test_no_match(rxnorm):
    """Test that a term normalizes to correct drug concept as a NO match."""
    # Misspelled name
    response = rxnorm.search("17-hydroxycorticosteroi")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0

    # Not storing foreign synonyms
    response = rxnorm.search("cisplatino")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0

    # Wrong Namespace
    response = rxnorm.search("rxnorm:3")
    assert response.match_type == MatchType.NO_MATCH

    # Test white space in between id
    response = rxnorm.search("rxcui: 3")
    assert response.match_type == MatchType.NO_MATCH

    # Should not store brand name concepts as identity record
    response = rxnorm.search("rxcui:202433")
    assert response.match_type == MatchType.NO_MATCH

    # Test empty query
    response = rxnorm.search("")
    assert response.match_type == MatchType.NO_MATCH
    assert len(response.records) == 0


def test_brand_name_to_concept(rxnorm):
    """Test that brand names are correctly linked to identity concept."""
    assert (
        rxnorm._query_handler.db.get_rxnorm_id_by_brand("rxcui:1041527") == "rxcui:161"
    )
    assert rxnorm._query_handler.db.get_rxnorm_id_by_brand("rxcui:218330") == "rxcui:44"


def test_xref_lookup(rxnorm, bifidobacterium_infantis, amiloride, compare_records):
    """Test that xref lookup resolves to correct concept."""
    response = rxnorm.search("mmsl:d07347")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 1
    compare_records(response.records[0], bifidobacterium_infantis)

    response = rxnorm.search("atc:C03DB01")
    assert response.match_type == MatchType.ASSOCIATED_WITH
    assert len(response.records) == 1
    compare_records(response.records[0], amiloride)


def test_phenobarbital(phenobarbital, rxnorm, compare_records):
    """Test phenobarbital search."""
    # Concept ID Match
    response = rxnorm.search("rxcui:8134")
    assert response.match_type == MatchType.CONCEPT_ID
    assert len(response.records) == 1
    compare_records(response.records[0], phenobarbital)

    # Label Match
    response = rxnorm.search(" phenobarbital")
    assert response.match_type == MatchType.LABEL
    assert len(response.records) == 1
    compare_records(response.records[0], phenobarbital)

    # Xref Match
    response = rxnorm.search("drugbank:DB01174")
    assert response.match_type == MatchType.XREF
    assert len(response.records) == 1
    compare_records(response.records[0], phenobarbital)


def test_meta_info(rxnorm):
    """Test that the meta field is correct."""
    response = rxnorm.search("RxNorm")
    assert response.source_meta_.data_license == "UMLS Metathesaurus"
    assert (
        response.source_meta_.data_license_url
        == "https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html"
    )
    assert isodate.parse_date(response.source_meta_.version)
    assert response.source_meta_.data_url.startswith(
        "https://download.nlm.nih.gov/umls/kss/rxnorm/"
    )
    assert not response.source_meta_.rdp_url
    assert response.source_meta_.data_license_attributes == {
        "non_commercial": False,
        "share_alike": False,
        "attribution": True,
    }
