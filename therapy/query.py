"""This module provides methods for handling queries"""
import re
from typing import List, Dict
from uvicorn.config import logger
from therapy import database, models, schemas # noqa F401
from therapy.etl import ChEMBL, Wikidata  # noqa F401
from therapy.database import Base, engine, SessionLocal
from therapy.models import Therapy, Alias, OtherIdentifier, TradeName, \
    Meta  # noqa F401
from therapy.schemas import Drug, MetaResponse, MatchType, SourceName, \
    NamespacePrefix
from sqlalchemy.orm import Session


class InvalidParameterException(Exception):
    """Exception for invalid parameter args provided by the user"""

    def __init__(self, message):
        """Create new instance

        Args:
            message: string describing the nature of the error
        """
        super().__init__(message)


def get_db():
    """Create a new SQLAlchemy SessionLocal that will be used in a single
    request, and then close it once the request is finished
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def emit_warnings(query_str):
    """Emit warnings if query contains non breaking space characters."""
    warnings = None
    nbsp = re.search('\xa0|\u00A0|&nbsp;', query_str)
    if nbsp:
        warnings = {'nbsp': 'Query contains non breaking space characters.'}
        logger.warning(
            f'Query ({query_str}) contains non breaking space characters.'
        )
    return warnings


def fetch_meta(session: Session, src_name: str) -> MetaResponse:
    """Fetch metadata for src_name"""
    meta = session.query(Meta).filter(Meta.src_name == src_name).first()
    if meta:
        params = {
            'data_license': meta.data_license,
            'data_license_url': meta.data_license_url,
            'version': meta.version,
            'data_url': meta.data_url
        }
        return MetaResponse(**params)


def fetch_records(session: Session,
                  response: Dict[str, Dict],
                  concept_ids: List[str],
                  match_type: MatchType) -> Dict:
    """Return matched Drug records.

    Args:
        session: to call queries from
        response: in-progress response object to return to client.
        concept_ids: List of concept IDs to build from
        match_type: level of match current queries are evaluated as

    Returns response Dict with records filled in via provided concept IDs
    """
    for concept_id in concept_ids:
        therapy = session.query(Therapy). \
            filter(Therapy.concept_id == concept_id).first()

        other_identifiers = session.query(
            OtherIdentifier.chembl_id,
            OtherIdentifier.wikidata_id,
            OtherIdentifier.ncit_id,
            OtherIdentifier.drugbank_id,
            OtherIdentifier.rxnorm_id,
            OtherIdentifier.pubchemcompound_id,
            OtherIdentifier.pubchemsubstance_id,
            OtherIdentifier.casregistry_id
        ).filter(OtherIdentifier.concept_id == concept_id).first()
        if other_identifiers:
            other_identifiers = [c_id for c_id in other_identifiers if c_id]
        else:
            other_identifiers = []

        # TODO does this need to be fixed?
        aliases = [a.alias for a in session.query(Alias).filter(
            Alias.concept_id == concept_id)]

        trade_name = [t.trade_name for t in session.query(
            TradeName).filter(TradeName.concept_id == concept_id)]

        params = {
            'concept_identifier': concept_id,
            'label': therapy.label,
            'max_phase': therapy.max_phase,
            'withdrawn': therapy.withdrawn_flag,
            'other_identifiers': other_identifiers,
            'aliases': aliases if aliases != [None] else [],
            'trade_name': trade_name if trade_name != [None] else []
        }

        drug = Drug(**params)
        src_name = therapy.src_name
        matches = response['normalizer_matches']
        if src_name not in matches.keys():
            pass
        elif matches[src_name] is None:
            matches[src_name] = {
                'match_type': match_type,
                'records': [drug],
                'meta_': fetch_meta(session, src_name)
            }
        elif matches[src_name]['match_type'] == match_type:
            matches[src_name]['records'].append(drug)
    return response


# TODO: Response list?
def fill_no_matches(session: Session, resp: Dict) -> Dict:
    """Fill all empty normalizer_matches slots with NO_MATCH results"""
    for src_name in resp['normalizer_matches'].keys():
        if resp['normalizer_matches'][src_name] is None:
            resp['normalizer_matches'][src_name] = {
                'match_type': MatchType.NO_MATCH,
                'records': [],
                'meta_': fetch_meta(session, src_name)
            }
    return resp


def create_session() -> Session:
    """Create a session to access database."""
    engine.connect()
    Base.metadata.create_all(bind=engine)
    get_db()
    return SessionLocal()


def response_keyed(query: str, sources: List[str], session: Session):
    """Return response as dict where key is source name and value
    is a list of records

    TODO refactor to return as soon as response is complete,
    clean up fetch_records calls
    """
    resp = {
        'query': query,
        'warnings': emit_warnings(query),
        'normalizer_matches': {
            source: None for source in sources
        }
    }

    if query == '':
        resp = fill_no_matches(session, resp)

    # check concept ID match
    def namespace_prefix_match(q: str) -> bool:
        namespace_prefixes = [prefix.value for prefix in
                              NamespacePrefix.__members__.values()]
        return len(list(filter(lambda p: q.startswith(p),
                               namespace_prefixes))) == 1

    if namespace_prefix_match(query.lower()):
        case_sensitive_match = namespace_prefix_match(query)
        query_split = query.split(':')
        query_split[0] = query_split[0].lower()
        query = ':'.join(query_split)
        results = session.query(Therapy).filter(
            Therapy.concept_id.ilike(f"{query}")  # TODO: fix case?
        ).all()
        if results:
            concept_ids = [r.concept_id for r in results]
            if case_sensitive_match:
                resp = fetch_records(session, resp, concept_ids,
                                     MatchType.PRIMARY)
            else:
                resp = fetch_records(session, resp, concept_ids,
                                     MatchType.NAMESPACE_CASE_INSENSITIVE)

    # check label match
    results = session.query(Therapy).filter(Therapy.label == query).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        resp = fetch_records(session, resp, concept_ids, MatchType.PRIMARY)

    # check case-insensitive label match
    results = session.query(Therapy)\
                     .filter(Therapy.label.ilike(f"{query}"))\
                     .all()
    if results:
        concept_ids = [r.concept_id for r in results]
        resp = fetch_records(session, resp, concept_ids,
                             MatchType.CASE_INSENSITIVE_PRIMARY)

    # check alias match
    results = session.query(Alias).filter(Alias.alias == query).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids, MatchType.ALIAS)

    # check trade name match
    results = session.query(TradeName).filter(
        TradeName.trade_name == query).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids, MatchType.ALIAS)

    # check case-insensitive alias match
    results = session.query(Alias).filter(
        Alias.alias.ilike(f"%{query}%")).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids,
                      MatchType.CASE_INSENSITIVE_ALIAS)

    # check case-insensitive trade name match
    results = session.query(TradeName).filter(
        TradeName.trade_name.ilike(f"%{query}%")).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids,
                      MatchType.CASE_INSENSITIVE_ALIAS)

    # remaining sources get no match
    resp = fill_no_matches(session, resp)

    session.close()
    return resp


def response_list(query: str, sources: List[str], session: Session):
    """Return response as list, where the first key-value in each item
    is the source name
    """
    resp = {  # noqa F841
        'query': query,
        'warnings': emit_warnings(query),
        'normalizer_matches': []
    }


def normalize(query_str, keyed='false', incl='', excl='', **params):
    """Fetch normalized therapy objects.

    Args:
        query_str: query, a string, to search for
        keyed: bool - if true, return response as dict keying normalizer names
            to normalizer objects; otherwise, return list of normalizer objects
        incl: str containing comma-separated names of normalizers to use. Will
            exclude all other normalizers. Case-insensitive. Raises
            InvalidParameterException if both incl and excl args are provided,
            or if invalid normalizer names are given.
        excl: str containing comma-separated names of normalizers to exclude.
            Will include all other normalizers. Case-insensitive. Raises
            InvalidParameterException if both incl and excl args are provided,
            or if invalid normalizer names are given.

    Returns:
        Dict containing all matches found in normalizers.
    """
    sources = {name.value.lower(): name.value for name in
               SourceName.__members__.values()}

    if not incl and not excl:
        query_sources = sources.values()
    elif incl and excl:
        detail = "Cannot request both normalizer inclusions and exclusions"
        raise InvalidParameterException(detail)
    elif incl:
        req_sources = [n.strip() for n in incl.split(',')]
        invalid_sources = []
        query_sources = []
        for source in req_sources:
            if source.lower() in sources.keys():
                query_sources.append(sources[source.lower()])
            else:
                invalid_sources.append(source)
        if invalid_sources:
            detail = f"Invalid normalizer name(s): {invalid_sources}"
            raise InvalidParameterException(detail)
    else:
        req_exclusions = [n.strip() for n in excl.lower().split(',')]
        req_excl_dict = {r.lower: r for r in req_exclusions}
        invalid_sources = []
        query_sources = []
        for req_l, req in req_excl_dict.items():
            if req_l not in sources.keys():
                invalid_sources.append(req)
        for src_l, src in sources.items():
            if src_l not in req_excl_dict.keys():
                query_sources.append(src)
        if invalid_sources:
            detail = f"Invalider normalizer name(s): {invalid_sources}"
            raise InvalidParameterException(detail)

    session = create_session()
    query_str = query_str.strip()

    if keyed:
        return response_keyed(query_str, query_sources, session)
    else:
        return response_list()
