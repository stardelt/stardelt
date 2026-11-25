-- The pipeline's own schema. A table can only be created in a schema that already exists, and
-- the catalog's authorization answers `Access Denied: Cannot create table` when it does not
-- (measured 2026-11-25: bronze held shop, shop_dbt and shop_dev, no public_db, and the
-- pipeline's first run failed on exactly that message).
create schema if not exists bronze.public_db
