"""This module provides methods for handling queries."""
import re
from typing import List, Dict

from sqlalchemy import func
from uvicorn.config import logger
from therapy import database, models, schemas  # noqa F401
from therapy.database import engine, SessionLocal
from therapy.models import Therapy, Alias, OtherIdentifier, TradeName, \
    Meta  # noqa F401
from therapy.schemas import Drug, MetaResponse, MatchType, SourceName, \
    NamespacePrefix, SourceIDAfterNamespace
from sqlalchemy.orm import Session


class InvalidParameterException(Exception):
    """Exception for invalid parameter args provided by the user."""

    def __init__(self, message):
        """Create new instance

        Args:
            message: string describing the nature of the error
        """
        super().__init__(message)


def emit_warnings(query_str):
    """Emit warnings if query contains non breaking space characters."""
    warnings = None
    nbsp = re.search('\xa0|&nbsp;', query_str)
    if nbsp:
        warnings = {'nbsp': 'Query contains non breaking space characters.'}
        logger.warning(
            f'Query ({query_str}) contains non breaking space characters.'
        )
    return warnings


def fetch_meta(session: Session, src_name: str) -> MetaResponse:
    """Fetch metadata for src_name."""
    meta = session.query(Meta).filter(Meta.src_name == src_name).first()
    if meta:
        params = {
            'data_license': meta.data_license,
            'data_license_url': meta.data_license_url,
            'version': meta.version,
            'data_url': meta.data_url
        }
        return MetaResponse(**params)
    else:
        raise Exception(f"Couldn't find metadata for source name: {src_name}")


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

        other_identifiers = session.query(OtherIdentifier).filter(
            OtherIdentifier.concept_id == concept_id).all()
        if other_identifiers:
            other_identifiers = \
                [c_id.other_id for c_id in other_identifiers if c_id]
        else:
            other_identifiers = []

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
        matches = response['source_matches']
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


def fill_no_matches(session: Session, resp: Dict) -> Dict:
    """Fill all empty source_matches slots with NO_MATCH results."""
    for src_name in resp['source_matches'].keys():
        if resp['source_matches'][src_name] is None:
            resp['source_matches'][src_name] = {
                'match_type': MatchType.NO_MATCH,
                'records': [],
                'meta_': fetch_meta(session, src_name)
            }
    return resp


def is_resp_complete(response: Dict, sources: List[str]) -> bool:
    """Check that response has all sources."""
    for src in sources:
        if not response['source_matches'][src]:
            return False
    return True


def create_session() -> Session:
    """Create a session to access database."""
    engine.connect()
    return SessionLocal()


def response_keyed(query: str, sources: List[str], session: Session):
    """Return response as dict where key is source name and value
    is a list of records.

    TODO refactor to return as soon as response is complete,
    clean up fetch_records calls
    """
    resp = {
        'query': query,
        'warnings': emit_warnings(query),
        'source_matches': {
            source: None for source in sources
        }
    }
    print(resp)

    if query == '':
        resp = fill_no_matches(session, resp)

    # check concept ID match
    def namespace_prefix_match(q: str) -> bool:
        namespace_prefixes = [prefix.value for prefix in
                              NamespacePrefix.__members__.values()]
        return len(list(filter(lambda p: q.startswith(p),
                               namespace_prefixes))) == 1

    # check that id after namespace match
    def id_after_namespace_match(q: str) -> bool:
        id_after_namespaces = [c_id.value for c_id in
                               SourceIDAfterNamespace.__members__.values()]
        return len(list(filter(lambda p: q.startswith(p),
                               id_after_namespaces))) == 1

    # Check concept ID
    if namespace_prefix_match(query.lower()) or\
            id_after_namespace_match(query.upper()):
        query_split = query.split(':')
        query_split[0] = query_split[0].lower()
        query = ':'.join(query_split)
        results = session.query(Therapy).filter(
            Therapy.concept_id.ilike(f"%{query}")
        ).all()

        if results:
            concept_ids = [r.concept_id for r in results]
            resp = fetch_records(session, resp, concept_ids,
                                 MatchType.CONCEPT_ID)

    if is_resp_complete(resp, sources):
        return resp

    # check label match
    results = session.query(Therapy).filter(func.lower(
        Therapy.label) == f"{query.lower()}").all()
    if results:
        concept_ids = [r.concept_id for r in results]
        resp = fetch_records(session, resp, concept_ids,
                             MatchType.PRIMARY_LABEL)

    if is_resp_complete(resp, sources):
        return resp

    # check alias match
    results = session.query(Alias).filter(
        func.lower(Alias.alias) == f"{query.lower()}").all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids,
                      MatchType.ALIAS)

    if is_resp_complete(resp, sources):
        return resp

    # check trade name match
    results = session.query(TradeName).filter(
        func.lower(TradeName.trade_name) == f"{query.lower()}").all()
    if results:
        concept_ids = [r.concept_id for r in results]
        fetch_records(session, resp, concept_ids,
                      MatchType.TRADE_NAME)

    if is_resp_complete(resp, sources):
        return resp

    # remaining sources get no match
    resp = fill_no_matches(session, resp)

    return resp


def response_list(query: str, sources: List[str], session: Session) -> Dict:
    """Return response as list, where the first key-value in each item
    is the source name.
    """
    response_dict = response_keyed(query, sources, session)
    source_list = []
    for src_name in response_dict['source_matches'].keys():
        src = {
            "source": src_name,
        }
        to_merge = response_dict['source_matches'][src_name]
        src.update(to_merge)

        source_list.append(src)
    response_dict['source_matches'] = source_list

    return response_dict


def normalize(query_str, keyed=False, incl='', excl='', **params):
    """Fetch normalized therapy objects.

    Args:
        query_str: query, a string, to search for
        keyed: bool - if true, return response as dict keying source names
            to source objects; otherwise, return list of source objects
        incl: str containing comma-separated names of sources to use. Will
            exclude all other sources. Case-insensitive. Raises
            InvalidParameterException if both incl and excl args are provided,
            or if invalid source names are given.
        excl: str containing comma-separated names of source to exclude.
            Will include all other source. Case-insensitive. Raises
            InvalidParameterException if both incl and excl args are provided,
            or if invalid source names are given.

    Returns:
        Dict containing all matches found in sources.
    """
    sources = {name.value.lower(): name.value for name in
               SourceName.__members__.values()}

    if not incl and not excl:
        query_sources = sources.values()
    elif incl and excl:
        detail = "Cannot request both source inclusions and exclusions."
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
            detail = f"Invalid source name(s): {invalid_sources}"
            raise InvalidParameterException(detail)
    else:
        req_exclusions = [n.strip() for n in excl.lower().split(',')]
        req_excl_dict = {r.lower(): r for r in req_exclusions}
        invalid_sources = []
        query_sources = []
        for req_l, req in req_excl_dict.items():
            if req_l not in sources.keys():
                invalid_sources.append(req)
        for src_l, src in sources.items():
            if src_l not in req_excl_dict.keys():
                query_sources.append(src)
        if invalid_sources:
            detail = f"Invalid source name(s): {invalid_sources}"
            raise InvalidParameterException(detail)

    session = create_session()
    query_str = query_str.strip()

    if keyed:
        resp = response_keyed(query_str, query_sources, session)
    else:
        resp = response_list(query_str, query_sources, session)

    session.close()
    return resp
