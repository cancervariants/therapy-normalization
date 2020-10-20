"""This module provides methods for handling queries"""
import re
from typing import List, Dict
from uvicorn.config import logger
from therapy import database, models, schemas # noqa F401
from therapy.etl import ChEMBL, Wikidata  # noqa F401
from therapy.database import Base, engine, SessionLocal
from therapy.models import Therapy, Alias, OtherIdentifier, TradeName, \
    Meta  # noqa F401
from therapy.schemas import Drug, MetaResponse, MatchType
from sqlalchemy.orm import Session

# session = SessionLocal() TODO can we just do this out here


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
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


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
            OtherIdentifier.wikidata_id, OtherIdentifier.chembl_id,
            OtherIdentifier.casregistry_id, OtherIdentifier.drugbank_id,
            OtherIdentifier.ncit_id, OtherIdentifier.pubchemcompound_id,
            OtherIdentifier.pubchemsubstance_id, OtherIdentifier.rxnorm_id
        ).first()

        aliases = [a.alias for a in session.query(Alias).filter(
            Alias.concept_id == concept_id)]

        trade_name = [t.trade_name for t in session.query(
            TradeName).filter(TradeName.concept_id == concept_id)]

        params = {
            'concept_identifier': concept_id,
            'label': therapy.label,
            'max_phase': therapy.max_phase,
            'withdrawn': therapy.withdrawn_flag,
            # TODO FIX
            'other_identifiers': [c_id for c_id in other_identifiers if c_id],
            'aliases': aliases if aliases != [None] else [],
            'trade_name': trade_name if trade_name != [None] else []
        }

        drug = Drug(**params)
        src_name = therapy.src_name
        if src_name not in response['normalizer_matches'].keys():
            pass
        elif response['normalizer_matches'][src_name] is None:
            response['normalizer_matches'][src_name] = {
                'match_type': match_type,
                'records': [drug],
                'meta_': fetch_meta(session, src_name)
            }
        elif response['normalizer_matches'][src_name]['match_type']\
                == match_type:
            # TODO add diff kinds of matches (eg alias vs tradename)?
            response['normalizer_matches'][src_name]['records'].append(drug)
    return response


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


# TODO: Case insensitivity for sources
def response_keyed(query: str, sources: List[str]):
    """Return response as dict where key is source name and value
    is a list of records

    TODO refactor to return as soon as response is complete,
    clean up fetch_records calls
    """
    query = query.strip()

    engine.connect()
    Base.metadata.create_all(bind=engine)
    get_db()
    session = SessionLocal()

    resp = {  # noqa F841
        'query': query,
        'warnings': emit_warnings(query),
        'normalizer_matches': {
            source: None for source in sources
        }
    }

    if query == '':
        resp = fill_no_matches(session, resp)

    # first check if therapies.concept_id = query
    def namespace_prefix_match(q: str) -> bool:
        namespace_prefixes = ['chembl', 'wikidata']
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

    # check if therapies.label = query
    results = session.query(Therapy).filter(Therapy.label == query).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        resp = fetch_records(session, resp, concept_ids, MatchType.PRIMARY)

    # check if therapies.label ILIKE query
    results = session.query(Therapy)\
                     .filter(Therapy.label.ilike(f"{query}"))\
                     .all()
    if results:
        concept_ids = [r.concept_id for r in results]
        resp = fetch_records(session, resp, concept_ids,
                             MatchType.CASE_INSENSITIVE_PRIMARY)

    # check if aliases.alias = query
    results = session.query(Alias).filter(Alias.alias == query).all()
    if results:
        concept_ids = [r.concept_ids for r in results]
        fetch_records(session, resp, concept_ids, MatchType.ALIAS)

    # check if trade_names.trade_name = query TODO special match type?
    results = session.query(TradeName).filter(
        TradeName.trade_name == query).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids, MatchType.ALIAS)

    # check if aliases.alias ILIKE query
    results = session.query(Alias).filter(
        Alias.alias.ilike(f"%{query}%")).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids,
                      MatchType.CASE_INSENSITIVE_ALIAS)

    # check if trade_names.trade_name ILIKE query
    results = session.query(TradeName).filter(
        TradeName.trade_name.ilike(f"%{query}%")).all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids,
                      MatchType.CASE_INSENSITIVE_ALIAS)

    # return NO MATCH
    resp = fill_no_matches(session, resp)
    session.close()
    return resp


def response_list():
    """Return response as list, where the first key-value in each item
    is the source name
    """
    pass


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
    sources = ['wikidata', 'chembl']

    if not incl and not excl:
        query_sources = sources
    elif incl and excl:
        detail = "Cannot request both normalizer inclusions and exclusions"
        raise InvalidParameterException(detail)
    elif incl:
        req_sources = incl.lower().split(',')
        req_sources = [n.strip() for n in req_sources]
        invalid_sources = []
        for source in req_sources:
            if source not in sources:
                invalid_sources.append(source)
        if invalid_sources:
            detail = f"Invalid normalizer name(s): {invalid_sources}"
            raise InvalidParameterException(detail)
        query_sources = req_sources
    else:
        req_exclusions = incl.lower().split(',')
        req_exclusions = [n.strip() for n in req_exclusions]
        query_sources = list(filter(
            lambda s: s not in req_exclusions,
            sources
        ))
        if len(query_sources) > len(sources) - len(req_exclusions):
            invalid_names = set(req_exclusions).difference(sources)
            detail = f"Invalider normalizer name(s): {invalid_names}"
            raise InvalidParameterException(detail)

    if keyed:
        return response_keyed(query_str, query_sources)
    else:
        return response_list()
    # if keyed:
    #     resp['normalizer_matches'] = dict()
    #     for normalizer in query_normalizers:
    #         results = normalizer.normalize(query_str)
    #         resp['normalizer_matches'][normalizer.__class__.__name__] = {
    #             'match_type': results.match_type,
    #             'records': results.records,
    #             'meta_': results.meta_._asdict(),
    #         }
    # else:
    #     resp['normalizer_matches'] = list()
    #     for normalizer in query_normalizers:
    #         results = normalizer.normalize(query_str)
    #         resp['normalizer_matches'].append({
    #             'normalizer': normalizer.__class__.__name__,
    #             'match_type': results.match_type,
    #             'records': results.records,
    #             'meta_': results.meta_._asdict(),
    #         })
    # return resp


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
