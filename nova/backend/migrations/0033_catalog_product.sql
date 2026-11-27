-- The data-product lifecycle of a documented table (stardelt/stardelt#37, slice 3): the
-- Iceberg properties `stardelt.lifecycle` and `stardelt.replaced-by`, copied here by
-- catalog_doc_store::record on every documentation edit. NULL means "not a data product".
ALTER TABLE catalog_doc ADD COLUMN lifecycle text;
ALTER TABLE catalog_doc ADD COLUMN replaced_by text;
CREATE INDEX catalog_doc_lifecycle ON catalog_doc (catalog, lifecycle)
    WHERE lifecycle IS NOT NULL;
