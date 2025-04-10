CREATE TABLE therapy_sources (
    name VARCHAR(127) PRIMARY KEY,
    data_license TEXT NOT NULL,
    data_license_url TEXT NOT NULL,
    version TEXT NOT NULL,
    data_url JSON NOT NULL,
    rdp_url TEXT,
    data_license_nc BOOLEAN NOT NULL,
    data_license_attr BOOLEAN NOT NULL,
    data_license_sa BOOLEAN NOT NULL
);
-- see also: delete_normalized_concepts.sql
CREATE TABLE therapy_merged (
    concept_id VARCHAR(127) PRIMARY KEY,
    name TEXT,
    xrefs TEXT [],
    aliases TEXT [],
    trade_names TEXT [],
    approval_ratings TEXT [],
    approval_years TEXT [],
    indications JSON []
);
CREATE TABLE therapy_concepts (
    concept_id VARCHAR(127) PRIMARY KEY,
    source VARCHAR(127) NOT NULL REFERENCES therapy_sources (name),
    name TEXT,
    approval_ratings TEXT [],
    approval_years TEXT [],
    indications JSON [],
    merge_ref VARCHAR(127) REFERENCES therapy_merged (concept_id)
);
CREATE TABLE therapy_xrefs (
    id SERIAL PRIMARY KEY,
    xref TEXT NOT NULL,
    concept_id VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
);
CREATE TABLE therapy_aliases (
    id SERIAL PRIMARY KEY,
    alias TEXT NOT NULL,
    concept_id VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
);
CREATE TABLE therapy_trade_names (
    id SERIAL PRIMARY KEY,
    trade_name TEXT NOT NULL,
    concept_ID VARCHAR(127) NOT NULL REFERENCES therapy_concepts (concept_id)
);
