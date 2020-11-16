"""This module provides methods for handling queries."""
import re
from typing import List, Dict

from uvicorn.config import logger
from therapy.database import DB
from therapy.schemas import Drug, Meta, MatchType, SourceName, \
    NamespacePrefix, SourceIDAfterNamespace
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# use to fetch source name from schema based on concept id namespace
PREFIX_LOOKUP = {v.value: SourceName[k].value
                 for k, v in NamespacePrefix.__members__.items()
                 if k in SourceName.__members__.keys()}

# use to generate namespace prefix from source ID value
NAMESPACE_LOOKUP = {v.value.lower(): NamespacePrefix[k].value
                    for k, v in SourceIDAfterNamespace.__members__.items()
                    if v.value != ''}

# use to identify queries that could be concept IDs
IDS_AFTER_NAMESPACE = {c.value.lower()
                       for c in SourceIDAfterNamespace.__members__.values()
                       if c.value != ''}


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


def fetch_meta(src_name: str) -> Meta:
    """Fetch metadata for src_name."""
    if src_name in DB.cached_sources.keys():
        return DB.cached_sources[src_name]
    else:
        table = DB.db.Table('Metadata')
        try:
            db_response = table.get_item(Key={'src_name': src_name})
        except ClientError as e:
            print(e.response['Error']['Message'])
        response = Meta(**db_response['Item'])
        DB.cached_sources[src_name] = response
        return response


# TODO refactor so that concept ID match doesn't have to go thru this
# process twice
def fetch_records(response: Dict[str, Dict],
                  concept_ids: List[str],
                  match_type: MatchType) -> Dict:
    """Return matched Drug records.

    Args:
        response: in-progress response object to return to client.
        concept_ids: List of concept IDs to build from. Should be all lower-
            case.
        match_type: level of match current queries are evaluated as

    Returns response Dict with records filled in via provided concept IDs
    """
    table = DB.db.Table('Therapies')
    for concept_id in concept_ids:
        try:
            pk = f'{concept_id.lower()}##identity'
            filter_exp = Key('label_and_type').eq(pk)
            match = table.query(KeyConditionExpression=filter_exp)['Items'][0]
        except ClientError as e:
            print(e.response['Error']['Message'])

        del match['label_and_type']

        drug = Drug(**match)
        src_name = PREFIX_LOOKUP[concept_id.split(':')[0]]

        matches = response['source_matches']
        if src_name not in matches.keys():
            pass
        elif matches[src_name] is None:
            matches[src_name] = {
                'match_type': match_type,
                'records': [drug],
                'meta_': fetch_meta(src_name)
            }
        elif matches[src_name]['match_type'] == match_type:
            matches[src_name]['records'].append(drug)
    return response


def fill_no_matches(resp: Dict) -> Dict:
    """Fill all empty source_matches slots with NO_MATCH results."""
    for src_name in resp['source_matches'].keys():
        if resp['source_matches'][src_name] is None:
            resp['source_matches'][src_name] = {
                'match_type': MatchType.NO_MATCH,
                'records': [],
                'meta_': fetch_meta(src_name)
            }
    return resp


def is_resp_complete(response: Dict, sources: List[str]) -> bool:
    """Check that response has all sources."""
    for src in sources:
        if not response['source_matches'][src]:
            return False
    return True


def response_keyed(query: str, sources: List[str]):
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
    table = DB.db.Table('Therapies')

    if query == '':
        resp = fill_no_matches(resp)
        return resp

    # check concept ID match
    def namespace_prefix_match(q: str) -> bool:
        namespace_prefixes = [prefix.value for prefix in
                              NamespacePrefix.__members__.values()]
        return len(list(filter(lambda p: q.startswith(p),
                               namespace_prefixes))) == 1

    # check that id after namespace match
    def id_after_namespace_match(q: str) -> bool:
        return len(list(filter(lambda p: q.startswith(p),
                               IDS_AFTER_NAMESPACE))) == 1

    query_l = query.lower()

    # Check concept ID by namespace prefix match
    # Assumes concept IDs have distinct source ID after prefix matches
    if namespace_prefix_match(query.lower()):
        pk = f'{query_l}##identity'
        filter_exp = Key('label_and_type').eq(pk)
        try:
            db_response = table.query(KeyConditionExpression=filter_exp)
        except ClientError as e:
            print(e.response['Error']['Message'])
        if len(db_response['Items']) > 0:
            concept_ids = [i['concept_id'] for i in db_response['Items']]
            resp = fetch_records(resp, concept_ids, MatchType.CONCEPT_ID)
    elif id_after_namespace_match(query_l):
        for p in IDS_AFTER_NAMESPACE:
            if query_l.startswith(p):
                pk = f'{NAMESPACE_LOOKUP[p].lower()}:{query_l}##identity'
                filter_exp = Key('label_and_type').eq(pk)
                try:
                    db_response = table.query(
                        KeyConditionExpression=filter_exp
                    )
                except ClientError as e:
                    print(e.response['Error']['Message'])
                if len(db_response['Items']) > 0:
                    concept_ids = [i['concept_id']
                                   for i in db_response['Items']]
                    resp = fetch_records(resp, concept_ids,
                                         MatchType.CONCEPT_ID)
                break
    if is_resp_complete(resp, sources):
        return resp

    # check label and trade_name match
    try:
        filter_exp = Key('label_and_type').eq(f'{query_l}##label')
        db_response = table.query(KeyConditionExpression=filter_exp)
        items = db_response['Items'][:]
        filter_exp = Key('label_and_type').eq(f'{query_l}##trade_name')
        db_response = table.query(KeyConditionExpression=filter_exp)
        items += db_response['Items'][:]
        if len(items) > 0:
            concept_ids = [i['concept_id'] for i in items]
            resp = fetch_records(resp, concept_ids, MatchType.PRIMARY_LABEL)
    except ClientError as e:
        print(e.response['Error']['Message'])
    if is_resp_complete(resp, sources):
        return resp

    # check alias match
    try:
        filter_exp = Key('label_and_type').eq(f'{query_l}##alias')
        db_response = table.query(KeyConditionExpression=filter_exp)
        if 'Items' in db_response.keys():
            concept_ids = [i['concept_id'] for i in db_response['Items']]
            resp = fetch_records(resp, concept_ids, MatchType.ALIAS)
    except ClientError as e:
        print(e.response['Error']['Message'])
    if is_resp_complete(resp, sources):
        return resp

    # remaining sources get no match
    resp = fill_no_matches(resp)

    return resp


def response_list(query: str, sources: List[str]) -> Dict:
    """Return response as list, where the first key-value in each item
    is the source name.
    """
    response_dict = response_keyed(query, sources)
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
    # sources = {name.value.lower(): name.value for name in
    #            SourceName.__members__.values()}

    # TODO testing remove
    sources = {"ncit": "NCIt", "wikidata": "Wikidata"}

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

    query_str = query_str.strip()

    if keyed:
        resp = response_keyed(query_str, query_sources)
    else:
        resp = response_list(query_str, query_sources)

    return resp
