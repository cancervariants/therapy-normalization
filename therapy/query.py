"""This module provides methods for handling queries."""
import re
from typing import List, Dict, Set

from uvicorn.config import logger
from therapy.database import THERAPIES_TABLE, METADATA_TABLE, cached_sources
from therapy.schemas import Drug, Meta, MatchType, SourceName, \
    NamespacePrefix, SourceIDAfterNamespace
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# use to fetch source name from schema based on concept id namespace
# e.g. {'chembl': 'ChEMBL'}
PREFIX_LOOKUP = {v.value: SourceName[k].value
                 for k, v in NamespacePrefix.__members__.items()
                 if k in SourceName.__members__.keys()}

# use to generate namespace prefix from source ID value
# e.g. {'q': 'wikidata'}
NAMESPACE_LOOKUP = {v.value.lower(): NamespacePrefix[k].value
                    for k, v in SourceIDAfterNamespace.__members__.items()
                    if v.value != ''}


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
    if src_name in cached_sources.keys():
        return cached_sources[src_name]
    else:
        try:
            db_response = METADATA_TABLE.get_item(Key={'src_name': src_name})
            print(db_response)
            response = Meta(**db_response['Item'])
            cached_sources[src_name] = response
            return response
        except ClientError as e:
            print(e.response['Error']['Message'])


def add_record(response: Dict[str, Dict],
               item: Dict,
               match_type: MatchType) -> (Dict, str):
    """Add individual record (i.e. Item in DynamoDB) to response object

    Args:
        response: in-progress response object to return to client
        item: Item retrieved from DynamoDB
        match_type: type of query match

    Returns:
        Tuple containing updated response object, and string containing name
        of the source of the match
    """
    del item['label_and_type']
    label_types = ['aliases', 'other_identifiers', 'trade_names']
    for label_type in label_types:
        if label_type not in item.keys():
            item[label_type] = []

    drug = Drug(**item)
    src_name = PREFIX_LOOKUP[drug.concept_id.split(':')[0]]

    matches = response['source_matches']
    if src_name not in matches.keys():
        pass
    elif matches[src_name] is None:
        matches[src_name] = {
            'match_type': match_type,
            'records': [drug],
            'meta_': fetch_meta(src_name)
        }
    elif matches[src_name]['match_type'].value == match_type.value:
        matches[src_name]['records'].append(drug)

    return (response, src_name)


def fetch_records(response: Dict[str, Dict],
                  concept_ids: List[str],
                  match_type: MatchType) -> (Dict, Set):
    """Return matched Drug records as a structured response for a given
    collection of concept IDs.

    Args:
        response: in-progress response object to return to client.
        concept_ids: List of concept IDs to build from. Should be all lower-
            case.
        match_type: level of match current queries are evaluated as

    Returns:
        response Dict with records filled in via provided concept IDs, and
        Set of source names of matched records
    """
    matched_sources = set()
    for concept_id in concept_ids:
        try:
            pk = f'{concept_id.lower()}##identity'
            filter_exp = Key('label_and_type').eq(pk)
            result = THERAPIES_TABLE.query(KeyConditionExpression=filter_exp)
            match = result['Items'][0]
            (response, src) = add_record(response, match, match_type)
            matched_sources.add(src)
        except ClientError as e:
            print(e.response['Error']['Message'])

    return (response, matched_sources)


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


def check_concept_id(query: str,
                     resp: Dict,
                     sources: Set[str]) -> (Dict, Set):
    """Check query for concept ID match. Should only find 0 or 1 matches,
    but stores them as a collection to be safe.

    Args:
        query: search string
        resp: in-progress response object to return to client
        sources: remaining unmatched sources

    Returns:
        Tuple with updated resp object and updated unmatched sources set
    """
    concept_id_items = []
    if [p for p in PREFIX_LOOKUP.keys() if query.startswith(p)]:
        pk = f'{query}##identity'
        filter_exp = Key('label_and_type').eq(pk)
        try:
            result = THERAPIES_TABLE.query(KeyConditionExpression=filter_exp)
            if len(result['Items']) > 0:
                concept_id_items += result['Items']
        except ClientError as e:
            print(e.response['Error']['Message'])
    for prefix in [p for p in NAMESPACE_LOOKUP.keys() if query.startswith(p)]:
        pk = f'{NAMESPACE_LOOKUP[prefix].lower()}:{query}##identity'
        filter_exp = Key('label_and_type').eq(pk)
        try:
            result = THERAPIES_TABLE.query(
                KeyConditionExpression=filter_exp
            )
            if len(result['Items']) > 0:  # TODO remove check?
                concept_id_items += result['Items']
        except ClientError as e:
            print(e.response['Error']['Message'])

    for item in concept_id_items:
        (resp, src_name) = add_record(resp, item, MatchType.CONCEPT_ID)
        sources = sources - {src_name}
    return (resp, sources)


def check_label_tn(query: str,
                   resp: Dict,
                   sources: Set[str]) -> (Dict, Set):
    """Check query for label/trade name match.

    Args:
        query: search string
        resp: in-progress response object to return to client
        sources: remaining unmatched sources

    Returns:
        Tuple with updated resp object and updated unmatched sources set
    """
    filter_exp = Key('label_and_type').eq(f'{query}##label')
    items = []
    try:
        db_response = THERAPIES_TABLE.query(KeyConditionExpression=filter_exp)
        items = db_response['Items'][:]
    except ClientError as e:
        print(e.response['Error']['Message'])

    filter_exp = Key('label_and_type').eq(f'{query}##trade_name')
    try:
        db_response = THERAPIES_TABLE.query(KeyConditionExpression=filter_exp)
        items += db_response['Items'][:]
    except ClientError as e:
        print(e.response['Error']['Message'])

    if len(items) > 0:
        concept_ids = {i['concept_id'] for i in items}
        (resp, matched_sources) = fetch_records(resp, concept_ids,
                                                MatchType.PRIMARY_LABEL)
        sources = sources - matched_sources
    return (resp, sources)


def check_alias(query: str,
                resp: Dict,
                sources: Set) -> (Dict, Set):
    """Check query for alias match.

    Args:
        query: search string
        resp: in-progress response object to return to client
        sources: remaining unmatched sources

    Returns:
        Tuple with updated resp object and updated unmatched sources set
    """
    filter_exp = Key('label_and_type').eq(f'{query}##alias')
    try:
        db_response = THERAPIES_TABLE.query(KeyConditionExpression=filter_exp)
        if 'Items' in db_response.keys():
            concept_ids = [i['concept_id'] for i in db_response['Items']]
            (resp, matched_sources) = fetch_records(resp, concept_ids,
                                                    MatchType.ALIAS)
            sources = sources - matched_sources
    except ClientError as e:
        print(e.response['Error']['Message'])
    return (resp, sources)


def response_keyed(query: str, sources: Set[str]) -> Dict:
    """Return response as dict where key is source name and value
    is a list of records. Corresponds to `keyed=true` API parameter.

    Args:
        query: string to match against
        sources: sources to match from

    Returns:
        Completed response object to return to client
    """
    resp = {
        'query': query,
        'warnings': emit_warnings(query),
        'source_matches': {
            source: None for source in sources
        }
    }
    if query == '':
        resp = fill_no_matches(resp)
        return resp
    query_l = query.lower()

    # check if concept ID match
    (resp, sources) = check_concept_id(query_l, resp, sources)
    if len(sources) == 0:
        return resp

    # check if label and trade_name match
    (resp, sources) = check_label_tn(query_l, resp, sources)
    if len(sources) == 0:
        return resp

    # check alias match
    (resp, sources) = check_alias(query_l, resp, sources)
    if len(sources) == 0:
        return resp

    # remaining sources get no match
    resp = fill_no_matches(resp)

    return resp


def response_list(query: str, sources: List[str]) -> Dict:
    """Return response as list, where the first key-value in each item
    is the source name. Corresponds to `keyed=false` API parameter.

    Args:
        query: string to match against
        sources: sources to match from

    Returns:
        Completed response object to return to client
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

    # TODO testing -- remove when dynamodb implementation complete
    sources = {"ncit": "NCIt", "wikidata": "Wikidata",
               "drugbank": "DrugBank"}

    if not incl and not excl:
        query_sources = set(sources.values())
    elif incl and excl:
        detail = "Cannot request both source inclusions and exclusions."
        raise InvalidParameterException(detail)
    elif incl:
        req_sources = [n.strip() for n in incl.split(',')]
        invalid_sources = []
        query_sources = set()
        for source in req_sources:
            if source.lower() in sources.keys():
                query_sources.add(sources[source.lower()])
            else:
                invalid_sources.append(source)
        if invalid_sources:
            detail = f"Invalid source name(s): {invalid_sources}"
            raise InvalidParameterException(detail)
    else:
        req_exclusions = [n.strip() for n in excl.lower().split(',')]
        req_excl_dict = {r.lower(): r for r in req_exclusions}
        invalid_sources = []
        query_sources = set()
        for req_l, req in req_excl_dict.items():
            if req_l not in sources.keys():
                invalid_sources.append(req)
        for src_l, src in sources.items():
            if src_l not in req_excl_dict.keys():
                query_sources.add(src)
        if invalid_sources:
            detail = f"Invalid source name(s): {invalid_sources}"
            raise InvalidParameterException(detail)

    query_str = query_str.strip()

    if keyed:
        resp = response_keyed(query_str, query_sources)
    else:
        resp = response_list(query_str, query_sources)

    return resp
