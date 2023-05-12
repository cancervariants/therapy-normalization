CREATE TABLE therapy_sources (
    name VARCHAR(127) PRIMARY KEY,
    data_license TEXT NOT NULL,
    data_license_url TEXT NOT NULL,
    version TEXT NOT NULL,
    data_url TEXT,
    rdp_url TEXT,
    data_license_nc BOOLEAN NOT NULL,
    data_license_attr BOOLEAN NOT NULL,
    data_license_sa BOOLEAN NOT NULL
);
-- see also: delete_normalized_concepts.sql
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
CREATE TABLE therapy_concepts (
    concept_id VARCHAR(127) PRIMARY KEY,
    source VARCHAR(127) NOT NULL REFERENCES therapy_sources (name),
    approval_ratings VARCHAR(127) [],
    approval_year VARCHAR(4) [],
    has_indication JSON [],
    merge_ref VARCHAR(127) REFERENCES therapy_merged (concept_id)
);
CREATE TABLE therapy_labels (
    id SERIAL PRIMARY KEY,
    label TEXT NOT NULL,
    concept_id VARCHAR(127) REFERENCES therapy_concepts (concept_id)
);
CREATE TABLE therapy_aliases (
    id SERIAL PRIMARY KEY,
    alias TEXT NOT NULL,
    concept_id VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
);
CREATE TABLE therapy_trade_names (
    id SERIAL PRIMARY KEY,
    trade_name TEXT NOT NULL,
    concept_id VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
);
CREATE TABLE therapy_xrefs (
    id SERIAL PRIMARY KEY,
    xref TEXT NOT NULL,
    concept_id VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
);
CREATE TABLE therapy_associations (
    id SERIAL PRIMARY KEY,
    associated_with TEXT NOT NULL,
    concept_ID VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
);
CREATE TABLE therapy_rx_brand_ids (
    id SERIAL PRIMARY KEY,
    rxcui VARCHAR(127),
    concept_id VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
)
