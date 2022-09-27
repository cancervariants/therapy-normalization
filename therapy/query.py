"""This module provides methods for handling queries."""
import re
from typing import Callable, List, Dict, Set, Tuple, TypeVar, Union, Any, Optional
from urllib.parse import quote
from datetime import datetime
import json

from uvicorn.config import logger
from botocore.exceptions import ClientError
from ga4gh.vrsatile.pydantic.vrsatile_models import TherapeuticDescriptor

from therapy import SOURCES, PREFIX_LOOKUP, ITEM_TYPES, NAMESPACE_LUIS
from therapy.database import Database
from therapy.schemas import BaseNormalizationService, Drug, SourceMeta, MatchType, \
    ServiceMeta, HasIndication, SourcePriority, SearchService, NormalizationService, \
    NamespacePrefix, SourceName, UnmergedNormalizationService, MatchesNormalized


NormService = TypeVar("NormService", bound=BaseNormalizationService)


class InvalidParameterException(Exception):
    """Exception for invalid parameter args provided by the user."""

    def __init__(self, message: str) -> None:
        """Create new instance

        :param str message: string describing the nature of the error
        """
        super().__init__(message)


class QueryHandler:
    """Class for normalizer management. Stores reference to database instance and
    normalizes query input.
    """

    def __init__(self, db_url: str = "", db_region: str = "us-east-2"):
        """Initialize Normalizer instance.

        :param str db_url: URL to database source.
        :param str db_region: AWS default region.
        """
        self.db = Database(db_url=db_url, region_name=db_region)

    def _emit_char_warnings(self, query_str: str) -> List[Dict]:
        """Emit warnings if query contains non breaking space characters.

        :param str query_str: query string
        :return: List of warnings (dicts)
        """
        warnings: List[Dict[str, str]] = []
        nbsp = re.search("\xa0|&nbsp;", query_str)
        if nbsp:
            warnings = [{
                "non_breaking_space_characters":
                    "Query contains non-breaking space characters"
            }]
            logger.warning(
                f"Query ({query_str}) contains non-breaking space characters."
            )
        return warnings

    def _fetch_meta(self, src_name: str) -> SourceMeta:
        """Fetch metadata for src_name.

        :param str src_name: name of source to get metadata for
        :return: SourceMeta object containing source metadata
        """
        if src_name in self.db.cached_sources.keys():
            return self.db.cached_sources[src_name]
        else:
            try:
                db_response = self.db.metadata.get_item(Key={"src_name": src_name})
            except ClientError as e:
                msg = e.response["Error"]["Message"]
                logger.error(msg)
                raise Exception(msg)
            try:
                response = SourceMeta(**db_response["Item"])
            except KeyError:
                msg = f"Metadata lookup failed for source {src_name}"
                logger.error(msg)
                raise Exception(msg)
            self.db.cached_sources[src_name] = response
            return response

    @staticmethod
    def _get_indication(indication_string: str) -> HasIndication:
        """Load indication data.
        :param str indication_string: dumped JSON string from db
        :return: complete HasIndication object
        """
        indication_values = json.loads(indication_string)
        return HasIndication(
            disease_id=indication_values[0],
            disease_label=indication_values[1],
            normalized_disease_id=indication_values[2],
            supplemental_info=indication_values[3]
        )

    def _add_record(self,
                    response: Dict[str, Dict],
                    item: Dict,
                    match_type: str) -> Tuple[Dict, str]:
        """Add individual record (i.e. Item in DynamoDB) to response object

        :param Dict[str, Dict] response: in-progress response object
        :param Dict item: Item retrieved from DynamoDB
        :param MatchType match_type: type of query match
        :return: Tuple containing updated response object, and string containing name of
            the source of the match
        """
        inds = item.get("has_indication")
        if inds:
            item["has_indication"] = [self._get_indication(i) for i in inds]

        drug = Drug(**item)
        src_name = item["src_name"]

        matches = response["source_matches"]
        if src_name not in matches.keys():
            pass
        elif matches[src_name] is None:
            matches[src_name] = {
                "match_type": MatchType[match_type.upper()],
                "records": [drug],
                "source_meta_": self._fetch_meta(src_name)
            }
        elif matches[src_name]["match_type"] == MatchType[match_type.upper()]:
            if drug.concept_id not in [r.concept_id for r
                                       in matches[src_name]["records"]]:
                matches[src_name]["records"].append(drug)

        return response, src_name

    def _fetch_records(self,
                       response: Dict[str, Dict],
                       concept_ids: Set[str],
                       match_type: str) -> Tuple[Dict, Set]:
        """Return matched Drug records as a structured response for a given collection
        of concept IDs.

        :param Dict[str, Dict] response: in-progress response object
        :param List[str] concept_ids: List of concept IDs to build from.  Should be all
            lower-case.
        :param str match_type: record should be assigned this type of match.
        :return: response Dict with records filled in via provided concept IDs, and Set
            of source names of matched records
        """
        matched_sources = set()
        for concept_id in concept_ids:
            try:
                match = self.db.get_record_by_id(concept_id.lower(),
                                                 case_sensitive=False)
                assert match, f"Unable to retrieve record for {concept_id}"
                (response, src) = self._add_record(response, match, match_type)
                matched_sources.add(src)
            except ClientError as e:
                logger.error(e.response["Error"]["Message"])

        return response, matched_sources

    def _fill_no_matches(self, resp: Dict[str, Any]) -> Dict:
        """Fill all empty source_matches slots with NO_MATCH results.

        :param Dict[str, Dict] resp: incoming response object
        :return: response object with empty source slots filled with NO_MATCH results
            and corresponding source metadata
        """
        for src_name in resp["source_matches"].keys():
            if resp["source_matches"][src_name] is None:
                resp["source_matches"][src_name] = {
                    "match_type": MatchType.NO_MATCH,
                    "records": [],
                    "source_meta_": self._fetch_meta(src_name)
                }
        return resp

    def _infer_namespace(self, query: str) -> Optional[Tuple[Dict, Dict]]:
        """Retrieve concept ID by inferring namespace. Attempts to match given query
        against known LUI patterns and performs concept ID lookup for all matches.
        :param str query: user-provided query string
        :return: Either tuple containing complete record and warnings if successful,
        or None if unsuccessful
        """
        inferred_records = []
        for pattern, source in NAMESPACE_LUIS:
            match = re.match(pattern, query)
            if match:
                if source == SourceName.DRUGSATFDA.value:
                    subspace, lui = match.groups()
                    namespace = f"drugsatfda.{subspace.lower()}"
                    inferred_id = f"{namespace}:{lui}"
                else:
                    namespace = NamespacePrefix[source.upper()].value
                    inferred_id = f"{namespace}:{query}"
                record = self.db.get_record_by_id(inferred_id, case_sensitive=False)
                if record:
                    inferred_records.append((record, namespace, inferred_id))
        if inferred_records:
            inferred_records.sort(key=lambda r: self._record_order(r[0]))
            return (
                inferred_records[0][0],
                {
                    "inferred_namespace": namespace,
                    "adjusted_query": inferred_records[0][2],
                    # probably not possible but just in case
                    "alternate_inferred_matches": [i[2] for i in inferred_records[1:]]
                }
            )
        else:
            return None

    def _check_concept_id(self, query: str, resp: Dict, sources: Set[str],
                          infer: bool = True) -> Tuple[Dict, Set]:
        """Check query for concept ID match. Should only find 0 or 1 matches.

        :param str query: search string
        :param Dict resp: in-progress response object to return to client
        :param Set[str] sources: remaining unmatched sources
        :param bool infer: if true, try to infer namespaces for IDs
        :return: Tuple with updated resp object and updated set of unmatched sources
        """
        records = []
        if infer:
            infer_response = self._infer_namespace(query)
            if infer_response:
                records.append(infer_response[0])
                resp["warnings"].append(infer_response[1])
        query_lower = query.lower()
        if [p for p in PREFIX_LOOKUP.keys() if query_lower.startswith(p)]:
            record = self.db.get_record_by_id(query, False)
            if record:
                records.append(record)
        for item in records:
            (resp, src_name) = self._add_record(resp, item,
                                                MatchType.CONCEPT_ID.name)
            sources = sources - {src_name}
        return resp, sources

    def _check_match_type(self, query: str, resp: Dict, sources: Set[str],
                          match_type: str) -> Tuple[Dict, Set]:
        """Check query for selected match type.
        :param str query: search string
        :param Dict resp: in-progress response object to return to client
        :param Set[str] sources: remaining unmatched sources
        :param str match_type: Match type to check for. Should be one of
        {'trade_name', 'label', 'alias', 'xref', 'associated_with'}
        :return: Tuple with updated resp object and updated set of unmatched
            sources
        """
        matches = self.db.get_records_by_type(query, match_type)
        if matches:
            concept_ids = {i["concept_id"] for i in matches}
            (resp, matched_srcs) = self._fetch_records(resp, concept_ids, match_type)
            sources = sources - matched_srcs
        return resp, sources

    def _response_keyed(self, query: str, sources: Set[str],
                        infer: bool = True) -> Dict:
        """Return response as dict where key is source name and value
        is a list of records. Corresponds to `keyed=true` API parameter.

        :param str query: string to match against
        :param Set[str] sources: sources to match from
        :param bool infer: if true, attempt to infer namespaces from IDs
        :return: completed response object to return to client
        """
        response: Dict[str, Union[None, str, List[Dict], Dict]] = {
            "query": query,
            "warnings": self._emit_char_warnings(query),
            "source_matches": {
                source: None for source in sources
            }
        }
        if query == "":
            response = self._fill_no_matches(response)
            return response
        query = query.strip()

        # check if concept ID match
        response, sources = self._check_concept_id(query, response, sources, infer)
        if len(sources) == 0:
            return response

        query = query.lower()
        for match_type in ITEM_TYPES.values():
            response, sources = self._check_match_type(query, response, sources,
                                                       match_type)
            if len(sources) == 0:
                return response

        # remaining sources get no match
        return self._fill_no_matches(response)

    def _response_list(self, query: str, sources: Set[str],
                       infer: bool = True) -> Dict:
        """Return response as list, where the first key-value in each item is the
        source name. Corresponds to `keyed=false` API parameter.

        :param str query: string to match against
        :param Set[str] sources: sources to match from
        :param bool infer: if true, attempt to infer namespaces from IDs
        :return: Completed response object to return to client
        """
        response_dict = self._response_keyed(query, sources, infer)
        source_list = []
        for src_name in response_dict["source_matches"].keys():
            src = {"source": src_name}
            to_merge = response_dict["source_matches"][src_name]
            src.update(to_merge)

            source_list.append(src)
        response_dict["source_matches"] = source_list

        return response_dict

    def search(self, query_str: str, keyed: bool = False, incl: str = "",
               excl: str = "", infer: bool = True) -> SearchService:
        """Fetch normalized therapy objects.

        :param str query_str: query, a string, to search for
        :param bool keyed: if true, return response as dict keying source names to
            source objects; otherwise, return list of source objects
        :param str incl: str containing comma-separated names of sources to use. Will
            exclude all other sources. Case-insensitive.
        :param str excl: str containing comma-separated names of source to exclude. Will
            include all other source. Case-insensitive.
        :param bool infer: if true, try to infer namespaces using known Local Unique
            Identifier patterns
        :return: dict containing all matches found in sources.
        :raises InvalidParameterException: if both incl and excl args are provided, or
            if invalid source names are given.
        """
        sources = dict()
        for k, v in SOURCES.items():
            if self.db.metadata.get_item(Key={"src_name": v}).get("Item"):
                sources[k] = v
        if not incl and not excl:
            query_sources = set(sources.values())
        elif incl and excl:
            detail = "Cannot request both source inclusions and exclusions."
            raise InvalidParameterException(detail)
        elif incl:
            req_sources = [n.strip() for n in incl.split(",")]
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
            req_exclusions = [n.strip() for n in excl.lower().split(",")]
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

        if keyed:
            response = self._response_keyed(query_str, query_sources, infer)
        else:
            response = self._response_list(query_str, query_sources, infer)

        response["service_meta_"] = ServiceMeta(
            response_datetime=datetime.now(),
        ).dict()
        return SearchService(**response)

    def _add_merged_meta(self, response: NormalizationService) -> NormalizationService:
        """Add source metadata to response object.

        :param NormalizationService response: in-progress response object
        :return: completed response object.
        """
        sources_meta = {}
        vod = response.therapeutic_descriptor

        xrefs = vod.xrefs or []  # type: ignore
        ids = [vod.therapeutic] + xrefs  # type: ignore
        for concept_id in ids:
            prefix = concept_id.split(":")[0]
            src_name = PREFIX_LOOKUP[prefix.lower()]
            if src_name not in sources_meta:
                sources_meta[src_name] = self._fetch_meta(src_name)
        response.source_meta_ = sources_meta  # type: ignore
        return response

    def _record_order(self, record: Dict) -> Tuple[int, str]:
        """Construct priority order for matching. Only called by sort().

        :param Dict record: individual record item in iterable to sort
        :return: tuple with rank value and concept ID
        """
        src = record["src_name"].upper()
        source_rank = SourcePriority[src]
        return source_rank, record["concept_id"]

    def _add_vod(self, response: NormalizationService, record: Dict, query: str,
                 match_type: MatchType) -> NormalizationService:
        """Format received DB record as VOD and update response object.
        :param NormalizationService response: in-progress response object
        :param Dict record: record as stored in DB
        :param str query: query string from user request
        :param MatchType match_type: type of match achieved
        :return: completed response object ready to return to user
        """
        vod = {
            "id": f"normalize.therapy:{quote(query.strip())}",
            "type": "TherapeuticDescriptor",
            "therapeutic": record["concept_id"],
            "label": record.get("label"),
            "extensions": [],
        }

        if "xrefs" in record:
            vod["xrefs"] = record["xrefs"]
        if "aliases" in record:
            vod["alternate_labels"] = record["aliases"]

        if any(filter(lambda f: f in record, ("approval_ratings",
                                              "approval_year",
                                              "has_indication"))):
            approv = {
                "type": "Extension",
                "name": "regulatory_approval",
                "value": {}
            }
            if "approval_ratings" in record:
                value = record.get("approval_ratings")
                if value:
                    approv["value"]["approval_ratings"] = value  # type: ignore
            if "approval_year" in record:
                value = record.get("approval_year")
                if value:
                    approv["value"]["approval_year"] = value  # type: ignore

            inds = record.get("has_indication", [])
            inds_list = []
            for ind_db in inds:
                indication = self._get_indication(ind_db)
                ind_value_obj: Dict[str, Optional[Union[str, List]]] = {
                    "id": indication.disease_id,
                    "type": "DiseaseDescriptor",
                    "label": indication.disease_label,
                    "disease": indication.normalized_disease_id,
                }
                if indication.supplemental_info:
                    ind_value_obj["extensions"] = [
                        {
                            "type": "Extension",
                            "name": k,
                            "value": v
                        }
                        for k, v in indication.supplemental_info.items()
                    ]
                inds_list.append(ind_value_obj)
            if inds_list:
                approv["value"]["has_indication"] = inds_list  # type: ignore
            vod["extensions"].append(approv)

        for field, name in (("trade_names", "trade_names"),
                            ("associated_with", "associated_with")):
            values = record.get(field)

            if values:
                vod["extensions"].append({
                    "type": "Extension",
                    "name": name,
                    "value": values
                })

        if not vod["extensions"]:
            del vod["extensions"]

        response.match_type = match_type
        response.therapeutic_descriptor = TherapeuticDescriptor(**vod)  # type: ignore
        response = self._add_merged_meta(response)
        return response

    def _resolve_merge(
        self, response: NormService, query: str, record: Dict,
        match_type: MatchType, callback: Callable
    ) -> NormService:
        """Given a record, return the corresponding normalized record

        :param NormalizationService response: in-progress response object
        :param str query: exact query as provided by user
        :param Dict record: record to retrieve normalized concept for
        :param MatchType match_type: type of match that returned these records
        :param Callable callback: response constructor method
        :return: Normalized response object
        """
        merge_ref = record.get("merge_ref")
        if merge_ref:
            # follow merge_ref
            merge = self.db.get_record_by_id(merge_ref, False, True)
            if merge is None:
                logger.error(f"Merge ref lookup failed for ref {record['merge_ref']} "
                             f"in record {record['concept_id']} from query `{query}`")
                return response
            else:
                return callback(response, merge, match_type)
        else:
            # record is sole member of concept group
            return callback(response, record, match_type)

    def _prepare_normalized_response(self, query: str) -> Dict[str, Any]:
        """Provide base response object for normalize endpoints.

        :param str query: user-provided query
        :return: basic normalization response boilerplate
        """
        return {
            "query": query,
            "match_type": MatchType.NO_MATCH,
            "warnings": self._emit_char_warnings(query),
            "service_meta_": ServiceMeta(response_datetime=datetime.now())
        }

    def _get_matches_by_type(self, query: str, match_type: str) -> List[Dict]:
        """Get matches list for match tier.
        :param str query: user-provided query
        :param str match_type: keyword of match type to check
        :return: List of records matching the query and match level
        """
        matching_refs = self.db.get_records_by_type(query, match_type)
        matching_records = [self.db.get_record_by_id(m["concept_id"], False)
                            for m in matching_refs]
        return sorted(matching_records, key=self._record_order)  # type: ignore

    def normalize(self, query: str, infer: bool = True) -> NormalizationService:
        """Return merged, normalized concept for given search term.

        :param str query: string to search against
        :param bool infer: if true, try to infer namespace for IDs
        :return: Normalized response object
        """
        # prepare basic response
        response = NormalizationService(**self._prepare_normalized_response(query))

        add_vod_curry = lambda res, rec, mat: self._add_vod(res, rec, query, mat)  # noqa: E501 E731
        return self._perform_normalized_lookup(response, query, infer, add_vod_curry)

    def _construct_drug_match(self, record: Dict) -> Drug:
        """Create individual Drug match for unmerged normalization endpoint.

        :param Dict record: record to add
        :return: completed Drug object
        """
        inds = record.get("has_indication")
        if inds:
            record["has_indication"] = [self._get_indication(i) for i in inds]
        return Drug(**record)

    def _add_normalized_records(self, response: UnmergedNormalizationService,
                                normalized_record: Dict,
                                match_type: MatchType) -> UnmergedNormalizationService:
        """Add individual records to unmerged normalize response.

        :param UnmergedNormalizationService response: in-progress response object
        :param Dict normalized_record: record associated with normalized concept,
            either merged or single identity
        :param MatchType match_type: type of match achieved
        :return: Completed response object
        """
        response.match_type = match_type
        response.normalized_concept_id = normalized_record["concept_id"]
        if normalized_record["item_type"] == "identity":
            record_source = SourceName[normalized_record["src_name"].upper()]
            response.source_matches[record_source] = MatchesNormalized(
                records=[self._construct_drug_match(normalized_record)],
                source_meta_=self._fetch_meta(record_source.value)
            )
        else:
            concept_ids = [normalized_record["concept_id"]] + \
                normalized_record.get("xrefs", [])
            for concept_id in concept_ids:
                record = self.db.get_record_by_id(concept_id, case_sensitive=False)
                if not record:
                    continue  # cover a few chemidplus edge cases
                record_source = SourceName[record["src_name"].upper()]
                drug = self._construct_drug_match(record)
                if record_source in response.source_matches:
                    response.source_matches[record_source].records.append(drug)
                else:
                    response.source_matches[record_source] = MatchesNormalized(
                        records=[drug],
                        source_meta_=self._fetch_meta(record_source.value)
                    )
        return response

    def _perform_normalized_lookup(self, response: NormService, query: str, infer: bool,
                                   response_builder: Callable) -> NormService:
        """Retrieve normalized concept, for use in normalization endpoints
        :param NormService response: in-progress response object
        :param str query: user-provided query
        :param bool infer: whether to try namespace inference
        :param Callable response_builder: response constructor callback method
        :return: completed service response object
        """
        if query == "":
            return response
        query_str = query.lower().strip()

        # check merged concept ID match
        record = self.db.get_record_by_id(query_str, case_sensitive=False, merge=True)
        if record:
            return response_builder(response, record, MatchType.CONCEPT_ID)

        # check concept ID match
        record = self.db.get_record_by_id(query_str, case_sensitive=False)
        if record:
            return self._resolve_merge(response, query, record, MatchType.CONCEPT_ID,
                                       response_builder)

        # check concept ID match with inferred namespace
        if infer:
            inferred_response = self._infer_namespace(query)
            if inferred_response:
                if response.warnings:
                    response.warnings.append(inferred_response[1])
                else:
                    response.warnings = [inferred_response[1]]
                return self._resolve_merge(response, query, inferred_response[0],
                                           MatchType.CONCEPT_ID,
                                           response_builder)

        # check other match types
        for match_type in ITEM_TYPES.values():
            matching_records = self._get_matches_by_type(query_str, match_type)

            # attempt merge ref resolution until successful
            for match in matching_records:
                assert match is not None
                record = self.db.get_record_by_id(match["concept_id"], False)
                if record:
                    match_type_value = MatchType[match_type.upper()]
                    return self._resolve_merge(response, query, record,
                                               match_type_value,
                                               response_builder)

        return response

    def normalize_unmerged(
        self, query: str, infer: bool = True
    ) -> UnmergedNormalizationService:
        """Return all source records under the normalized concept for the provided
        query string.

        :param str query: string to search against
        :param bool infer: if true, try to infer namespace for IDs
        :return: Normalized response object
        """
        response = UnmergedNormalizationService(
            source_matches={},
            **self._prepare_normalized_response(query)
        )
        return self._perform_normalized_lookup(response, query, infer,
                                               self._add_normalized_records)
