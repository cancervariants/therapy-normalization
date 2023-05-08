CREATE MATERIALIZED VIEW record_lookup_view AS
SELECT tc.concept_id,
       tl.label,
       ta.aliases,
       tas.associated_with,
       ttr.trade_names,
       tx.xrefs,
       tc.has_indication,
       tc.approval_ratings,
       tc.approval_year,
       tc.source,
       tc.merge_ref,
       lower(tc.concept_id) AS concept_id_lowercase
FROM therapy_concepts tc
FULL JOIN (
    SELECT ta_1.concept_id, array_agg(ta_1.alias) AS aliases
    FROM therapy_aliases ta_1
    GROUP BY ta_1.concept_id
) ta ON tc.concept_id::text = ta.concept_id::text
FULL JOIN (
    SELECT tas_1.concept_id, array_agg(tas_1.associated_with) AS associated_with
    FROM therapy_associations tas_1
    GROUP BY tas_1.concept_id
) tas ON tc.concept_id::text = tas.concept_id::text
FULL JOIN (
    SELECT ttr_1.concept_id, array_agg(ttr_1.trade_name) AS trade_names
    FROM therapy_trade_names ttr_1
    GROUP BY ttr_1.concept_id
) ttr ON tc.concept_id::text = ttr.concept_id::text
FULL JOIN therapy_labels tl ON tc.concept_id::text = tl.concept_id::text
FULL JOIN (
    SELECT tx_1.concept_id, array_agg(tx_1.xref) AS xrefs
    FROM therapy_xrefs tx_1
    GROUP BY tx_1.concept_id
) tx ON tc.concept_id::text = tx.concept_id::text;
