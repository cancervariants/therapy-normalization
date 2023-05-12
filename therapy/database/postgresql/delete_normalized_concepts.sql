-- some redundancy between here and create_tables.sql, drop_indexes.sql,
-- add_indexes.sql.
DROP INDEX IF EXISTS idx_tm_concept_id_low;
ALTER TABLE therapy_concepts DROP CONSTRAINT IF EXISTS therapy_concepts_merge_ref_fkey;
UPDATE therapy_concepts SET merge_ref = NULL;
DROP TABLE therapy_merged;
CREATE TABLE therapy_merged (
    concept_id VARCHAR(127) PRIMARY KEY,
    label TEXT,
    aliases TEXT [],
    associated_with TEXT [],
    trade_names TEXT [],
    xrefs TEXT [],
    approval_ratings VARCHAR(127) [],
    approval_year VARCHAR(4) [],
    has_indication JSON []
);
ALTER TABLE therapy_concepts ADD CONSTRAINT therapy_concepts_merge_ref_fkey
    FOREIGN KEY (merge_ref) REFERENCES therapy_merged (concept_id);
CREATE INDEX idx_tm_concept_id_low ON therapy_merged (lower(concept_id));

