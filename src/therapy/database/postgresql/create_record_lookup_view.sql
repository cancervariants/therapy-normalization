CREATE MATERIALIZED VIEW record_lookup_view AS
SELECT tc.concept_id,
       tc.name,
       tc.approval_ratings,
       tc.approval_years,
       tc.indications,
       ta.aliases,
       ttn.trade_names,
       tx.xrefs,
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
    SELECT ttn_1.concept_id, array_agg(ttn_1.trade_name) AS trade_names
    FROM therapy_trade_names ttn_1
    GROUP BY ttn_1.concept_id
) ttn ON tc.concept_id::text = ttn.concept_id::text
FULL JOIN (
    SELECT tx_1.concept_id, array_agg(tx_1.xref) AS xrefs
    FROM therapy_xrefs tx_1
    GROUP BY tx_1.concept_id
) tx ON tc.concept_id::text = tx.concept_id::text;
