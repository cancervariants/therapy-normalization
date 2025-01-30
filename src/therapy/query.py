"""Provides methods for handling queries."""

import datetime
import json
import re
from collections.abc import Callable
from typing import Any, TypeVar

from botocore.exceptions import ClientError
from disease.schemas import get_concept_mapping as get_disease_concept_mapping
from ga4gh.core.models import (
    Coding,
    ConceptMapping,
    Extension,
    MappableConcept,
    Relation,
    code,
)
from uvicorn.config import logger

from therapy import NAMESPACE_LUIS, PREFIX_LOOKUP, SOURCES
from therapy.database import AbstractDatabase
from therapy.schemas import (
    NAMESPACE_TO_SYSTEM_URI,
    BaseNormalizationService,
    HasIndication,
    MatchesNormalized,
    MatchType,
    NamespacePrefix,
    NormalizationService,
    RefType,
    SearchService,
    ServiceMeta,
    SourceName,
    SourcePriority,
    Therapy,
    UnmergedNormalizationService,
)

NormService = TypeVar("NormService", bound=BaseNormalizationService)


class InvalidParameterError(Exception):
    """Exception for invalid parameter args provided by the user."""


class QueryHandler:
    """Class for normalizer management. Stores reference to database instance and
    normalizes query input.
    """

    def __init__(self, database: AbstractDatabase) -> None:
        """Initialize QueryHandler instance. Requires a created database object to
        initialize. The most straightforward way to do this is via the ``create_db``
        method in the ``therapy.database`` module:

        >>> from therapy.query import QueryHandler
        >>> from therapy.database import create_db
        >>> q = QueryHandler(create_db())

        :param database: storage backend to search against
        """
        self.db = database

    def _emit_char_warnings(self, query_str: str) -> list[dict]:
        """Emit warnings if query contains non breaking space characters.

        :param str query_str: query string
        :return: List of warnings (dicts)
        """
        warnings: list[dict[str, str]] = []
        nbsp = re.search("\xa0|&nbsp;", query_str)
        if nbsp:
            warnings = [
                {
                    "non_breaking_space_characters": "Query contains non-breaking space characters"
                }
            ]
            logger.warning(
                f"Query ({query_str}) contains non-breaking space characters."
            )
        return warnings

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
            supplemental_info=indication_values[3],
        )

    def _add_record(
        self, response: dict[str, dict], item: dict, match_type: str
    ) -> tuple[dict, str]:
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

        drug = Therapy(**item)
        src_name = item["src_name"]

        matches = response["source_matches"]
        if src_name not in matches:
            pass
        elif matches[src_name] is None:
            matches[src_name] = {
                "match_type": MatchType[match_type.upper()],
                "records": [drug],
                "source_meta_": self.db.get_source_metadata(src_name),
            }
        elif (matches[src_name]["match_type"] == MatchType[match_type.upper()]) and (
            drug.concept_id not in [r.concept_id for r in matches[src_name]["records"]]
        ):
            matches[src_name]["records"].append(drug)

        return response, src_name

    def _fetch_records(
        self, response: dict[str, dict], concept_ids: set[str], match_type: str
    ) -> tuple[dict, set]:
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
                match = self.db.get_record_by_id(
                    concept_id.lower(), case_sensitive=False
                )
                if not match:
                    msg = f"Unable to retrieve record for {concept_id}"
                    raise KeyError(msg)
                (response, src) = self._add_record(response, match, match_type)
                matched_sources.add(src)
            except ClientError as e:
                logger.error(e.response["Error"]["Message"])

        return response, matched_sources

    def _fill_no_matches(self, resp: dict[str, Any]) -> dict:
        """Fill all empty source_matches slots with NO_MATCH results.

        :param Dict[str, Dict] resp: incoming response object
        :return: response object with empty source slots filled with NO_MATCH results
            and corresponding source metadata
        """
        for src_name in resp["source_matches"]:
            if resp["source_matches"][src_name] is None:
                resp["source_matches"][src_name] = {
                    "match_type": MatchType.NO_MATCH,
                    "records": [],
                    "source_meta_": self.db.get_source_metadata(src_name),
                }
        return resp

    def _infer_namespace(self, query: str) -> tuple[dict, dict] | None:
        """Retrieve concept ID by inferring namespace. Attempts to match given query
        against known LUI patterns and performs concept ID lookup for all matches.
        :param str query: user-provided query string
        :return: Either tuple containing complete record and warnings if successful,
        or None if unsuccessful
        """
        inferred_records = []
        namespace = None
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
        if inferred_records and namespace:
            inferred_records.sort(key=lambda r: self._record_order(r[0]))
            return (
                inferred_records[0][0],
                {
                    "inferred_namespace": namespace,
                    "adjusted_query": inferred_records[0][2],
                    # probably not possible but just in case
                    "alternate_inferred_matches": [i[2] for i in inferred_records[1:]],
                },
            )
        return None

    def _check_concept_id(
        self, query: str, resp: dict, sources: set[str], infer: bool = True
    ) -> tuple[dict, set]:
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
        if [p for p in PREFIX_LOOKUP if query_lower.startswith(p)]:
            record = self.db.get_record_by_id(query, False)
            if record:
                records.append(record)
        for item in records:
            (resp, src_name) = self._add_record(resp, item, MatchType.CONCEPT_ID.name)
            sources = sources - {src_name}
        return resp, sources

    def _check_match_type(
        self, query: str, resp: dict, sources: set[str], match_type: RefType
    ) -> tuple[dict, set]:
        """Check query for selected match type.

        :param query: search string
        :param resp: in-progress response object to return to client
        :param sources: remaining unmatched sources
        :param match_type: Match type to check for
        :return: Tuple with updated resp object and updated set of unmatched sources
        """
        matching_ids = self.db.get_refs_by_type(query, match_type)
        if matching_ids:
            (resp, matched_srcs) = self._fetch_records(
                resp, set(matching_ids), match_type
            )
            sources = sources - matched_srcs
        return resp, sources

    def _get_search_response(
        self, query: str, sources: set[str], infer: bool = True
    ) -> dict:
        """Return response as dict where key is source name and value
        is a list of records.

        :param str query: string to match against
        :param Set[str] sources: sources to match from
        :param bool infer: if true, attempt to infer namespaces from IDs
        :return: completed response object to return to client
        """
        response: dict[str, None | str | list[dict] | dict] = {
            "query": query,
            "warnings": self._emit_char_warnings(query),
            "source_matches": {source: None for source in sources},
        }
        if query == "":
            return self._fill_no_matches(response)
        query = query.strip()

        # check if concept ID match
        response, sources = self._check_concept_id(query, response, sources, infer)
        if len(sources) == 0:
            return response

        query = query.lower()
        for match_type in RefType:
            response, sources = self._check_match_type(
                query, response, sources, match_type
            )
            if len(sources) == 0:
                return response

        # remaining sources get no match
        return self._fill_no_matches(response)

    def search(
        self,
        query_str: str,
        incl: str = "",
        excl: str = "",
        infer: bool = True,
    ) -> SearchService:
        """Fetch normalized therapy objects.

        :param str query_str: query, a string, to search for
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
        sources = {}
        sources = {k: v for k, v in SOURCES.items() if self.db.get_source_metadata(v)}
        if not incl and not excl:
            query_sources = set(sources.values())
        elif incl and excl:
            detail = "Cannot request both source inclusions and exclusions."
            raise InvalidParameterError(detail)
        elif incl:
            req_sources = [n.strip() for n in incl.split(",")]
            invalid_sources = []
            query_sources = set()
            for source in req_sources:
                if source.lower() in sources:
                    query_sources.add(sources[source.lower()])
                else:
                    invalid_sources.append(source)
            if invalid_sources:
                detail = f"Invalid source name(s): {invalid_sources}"
                raise InvalidParameterError(detail)
        else:
            req_exclusions = [n.strip() for n in excl.lower().split(",")]
            req_excl_dict = {r.lower(): r for r in req_exclusions}
            invalid_sources = []
            query_sources = set()
            for req_l, req in req_excl_dict.items():
                if req_l not in sources:
                    invalid_sources.append(req)
            for src_l, src in sources.items():
                if src_l not in req_excl_dict:
                    query_sources.add(src)
            if invalid_sources:
                detail = f"Invalid source name(s): {invalid_sources}"
                raise InvalidParameterError(detail)

        response = self._get_search_response(query_str, query_sources, infer)

        response["service_meta_"] = ServiceMeta(
            response_datetime=datetime.datetime.now(tz=datetime.UTC),
        ).model_dump()
        return SearchService(**response)

    def _add_merged_meta(self, response: NormalizationService) -> NormalizationService:
        """Add source metadata to response object.

        :param NormalizationService response: in-progress response object
        :return: completed response object.
        """
        sources_meta = {}
        therapy = response.therapy

        sources = []
        for m in therapy.mappings or []:
            ns = m.coding.id.split(":")[0]
            if ns in PREFIX_LOOKUP:
                sources.append(PREFIX_LOOKUP[ns])

        for src in sources:
            if src not in sources_meta:
                sources_meta[SourceName[src.upper()]] = self.db.get_source_metadata(src)
        response.source_meta_ = sources_meta  # type: ignore[assignment]
        return response

    def _record_order(self, record: dict) -> tuple[int, str]:
        """Construct priority order for matching. Only called by sort().

        :param Dict record: individual record item in iterable to sort
        :return: tuple with rank value and concept ID
        """
        src = record["src_name"].upper()
        source_rank = SourcePriority[src]
        return source_rank, record["concept_id"]

    def _add_therapy(
        self,
        response: NormalizationService,
        record: dict,
        match_type: MatchType,
    ) -> NormalizationService:
        """Format received DB record as Mappable Concept and update response object.
        :param NormalizationService response: in-progress response object
        :param Dict record: record as stored in DB
        :param str query: query string from user request
        :param MatchType match_type: type of match achieved
        :return: completed response object ready to return to user
        """

        def _get_concept_mapping(
            concept_id: str,
            relation: Relation,
        ) -> ConceptMapping:
            """Create concept mapping for identifier

            ``system`` will use system prefix URL, OBO Foundry persistent URL (PURL), or
            source homepage, in that order of preference.

            :param concept_id: Concept identifier represented as a curie
            :param relation: SKOS mapping relationship, default is relatedMatch
            :raises ValueError: If source of concept ID is not a valid
                ``NamespacePrefix``
            :return: Concept mapping for identifier
            """
            source, source_code = concept_id.split(":")

            try:
                source = NamespacePrefix(source)
            except ValueError:
                try:
                    source = NamespacePrefix(source.upper())
                except ValueError as e:
                    err_msg = f"Namespace prefix not supported: {source}"
                    raise ValueError(err_msg) from e

            if source == NamespacePrefix.CHEBI:
                source_code = concept_id

            return ConceptMapping(
                coding=Coding(
                    id=concept_id,
                    code=code(source_code),
                    system=NAMESPACE_TO_SYSTEM_URI[source],
                ),
                relation=relation,
            )

        therapy_obj = MappableConcept(
            id=f"normalize.therapy.{record['concept_id']}",
            primaryCode=code(root=record["concept_id"]),
            conceptType="Therapy",
            label=record.get("label"),
        )

        xrefs = [record["concept_id"], *record.get("xrefs", [])]
        therapy_obj.mappings = [
            _get_concept_mapping(xref_id, relation=Relation.EXACT_MATCH)
            for xref_id in xrefs
        ]

        associated_with = record.get("associated_with", [])
        therapy_obj.mappings.extend(
            _get_concept_mapping(associated_with_id, relation=Relation.RELATED_MATCH)
            for associated_with_id in associated_with
        )

        extensions = []
        if "aliases" in record:
            extensions.append(Extension(name="aliases", value=record["aliases"]))

        if any(
            filter(
                lambda f: f in record,
                ("approval_ratings", "approval_year", "has_indication"),
            )
        ):
            approv_value = {}
            if "approval_ratings" in record:
                value = record.get("approval_ratings")
                if value:
                    approv_value["approval_ratings"] = value
            if "approval_year" in record:
                value = record.get("approval_year")
                if value:
                    approv_value["approval_year"] = value

            inds = record.get("has_indication", [])
            inds_list = []
            for ind_db in inds:
                indication = self._get_indication(ind_db)

                if indication.normalized_disease_id:
                    mappings = [
                        get_disease_concept_mapping(
                            concept_id=indication.normalized_disease_id,
                            relation=Relation.EXACT_MATCH,
                        )
                    ]
                else:
                    mappings = []
                ind_disease_obj = MappableConcept(
                    id=indication.disease_id,
                    conceptType="Disease",
                    label=indication.disease_label,
                    mappings=mappings or None,
                )

                if indication.supplemental_info:
                    ind_disease_obj.extensions = [
                        Extension(name=k, value=v)
                        for k, v in indication.supplemental_info.items()
                    ]
                inds_list.append(ind_disease_obj.model_dump(exclude_none=True))
            if inds_list:
                approv_value["has_indication"] = inds_list

            approv = Extension(name="regulatory_approval", value=approv_value)
            extensions.append(approv)

        trade_names = record.get("trade_names")
        if trade_names:
            extensions.append(Extension(name="trade_names", value=trade_names))

        if extensions:
            therapy_obj.extensions = extensions

        response.match_type = match_type
        response.therapy = therapy_obj
        return self._add_merged_meta(response)

    def _resolve_merge(
        self,
        response: NormService,
        query: str,
        record: dict,
        match_type: MatchType,
        callback: Callable,
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
                logger.error(
                    f"Merge ref lookup failed for ref {record['merge_ref']} "
                    f"in record {record['concept_id']} from query `{query}`"
                )
                return response
            return callback(response, merge, match_type)
        # record is sole member of concept group
        return callback(response, record, match_type)

    def _prepare_normalized_response(self, query: str) -> dict[str, Any]:
        """Provide base response object for normalize endpoints.

        :param str query: user-provided query
        :return: basic normalization response boilerplate
        """
        return {
            "query": query,
            "match_type": MatchType.NO_MATCH,
            "warnings": self._emit_char_warnings(query),
            "service_meta_": ServiceMeta(
                response_datetime=datetime.datetime.now(tz=datetime.UTC)
            ),
        }

    def normalize(self, query: str, infer: bool = True) -> NormalizationService:
        """Return merged, normalized concept for given search term.

        :param str query: string to search against
        :param bool infer: if true, try to infer namespace for IDs
        :return: Normalized response object
        """
        # prepare basic response
        response = NormalizationService(**self._prepare_normalized_response(query))

        return self._perform_normalized_lookup(
            response, query, infer, self._add_therapy
        )

    def _construct_drug_match(self, record: dict) -> Therapy:
        """Create individual Drug match for unmerged normalization endpoint.

        :param Dict record: record to add
        :return: completed Drug object
        """
        inds = record.get("has_indication")
        if inds:
            record["has_indication"] = [self._get_indication(i) for i in inds]
        return Therapy(**record)

    def _add_normalized_records(
        self,
        response: UnmergedNormalizationService,
        normalized_record: dict,
        match_type: MatchType,
    ) -> UnmergedNormalizationService:
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
                source_meta_=self.db.get_source_metadata(record_source),
            )
        else:
            concept_ids = [
                normalized_record["concept_id"],
                *normalized_record.get("xrefs", []),
            ]
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
                        source_meta_=self.db.get_source_metadata(record_source),
                    )
        return response

    def _perform_normalized_lookup(
        self, response: NormService, query: str, infer: bool, response_builder: Callable
    ) -> NormService:
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
            return self._resolve_merge(
                response, query, record, MatchType.CONCEPT_ID, response_builder
            )

        # check concept ID match with inferred namespace
        if infer:
            inferred_response = self._infer_namespace(query)
            if inferred_response:
                if response.warnings:
                    response.warnings.append(inferred_response[1])
                else:
                    response.warnings = [inferred_response[1]]
                return self._resolve_merge(
                    response,
                    query,
                    inferred_response[0],
                    MatchType.CONCEPT_ID,
                    response_builder,
                )

        # check other match types
        for match_type in RefType:
            matching_refs = self.db.get_refs_by_type(query_str, match_type)
            matching_records = [
                self.db.get_record_by_id(ref, False) for ref in matching_refs
            ]
            matching_records.sort(key=self._record_order)  # type: ignore[arg-type]

            # attempt merge ref resolution until successful
            for match in matching_records:
                if match is None:
                    raise ValueError
                record = self.db.get_record_by_id(match["concept_id"], False)
                if record:
                    match_type_value = MatchType[match_type.upper()]
                    return self._resolve_merge(
                        response, query, record, match_type_value, response_builder
                    )

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
            source_matches={}, **self._prepare_normalized_response(query)
        )
        return self._perform_normalized_lookup(
            response, query, infer, self._add_normalized_records
        )
