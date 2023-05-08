CREATE INDEX idx_t_concept_id_low ON therapy_concepts (lower(concept_id));
CREATE INDEX idx_tm_concept_id_low ON therapy_merged (lower(concept_id));
CREATE INDEX idx_tl_label_low ON therapy_labels (lower(label));
CREATE INDEX idx_ta_alias_low ON therapy_aliases (lower(alias));
CREATE INDEX idx_ttr_trade_name_low ON therapy_trade_names (lower(trade_name));
CREATE INDEX idx_tx_xref_low ON therapy_xrefs (lower(xref));
CREATE INDEX idx_t_as_association_low ON therapy_associations (lower(associated_with));
CREATE INDEX idx_rlv_concept_id_low ON record_lookup_view (lower(concept_id));
