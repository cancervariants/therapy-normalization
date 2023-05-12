CREATE MATERIALIZED VIEW unii_lookup_view AS
SELECT concept_id, unii FROM (
    SELECT concept_id, (SELECT unnest(array_agg(associated_with))) as unii
    FROM therapy_associations ta
    WHERE associated_with ILIKE 'unii:%%' AND concept_id ILIKE 'drugsatfda.%%'
    GROUP BY concept_id
    HAVING count(associated_with) = 1
) valid_dafda_uniis;
