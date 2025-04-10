ALTER TABLE therapy_aliases ADD CONSTRAINT therapy_aliases_concept_id_fkey
    FOREIGN KEY (concept_id) REFERENCES therapy_concepts (concept_id);
ALTER TABLE therapy_trade_names ADD CONSTRAINT therapy_associations_concept_id_fkey
    FOREIGN KEY (concept_id) REFERENCES therapy_concepts (concept_id);
ALTER TABLE therapy_xrefs ADD CONSTRAINT therapy_xrefs_concept_id_fkey
    FOREIGN KEY (concept_id) REFERENCES therapy_concepts (concept_id);
ALTER TABLE therapy_associated_with ADD CONSTRAINT therapy_associated_with_concept_id_fkey
    FOREIGN KEY (concept_id) REFERENCES therapy_concepts (concept_id);
