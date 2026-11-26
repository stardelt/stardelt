-- The tables whose freshness promise is watched (#402 point 3, slice 1). A row exists only
-- for a table whose promise was written through Nova with at least one alert destination;
-- the promise itself stays in the Iceberg property `stardelt.freshness`.
CREATE TABLE table_promise (
    catalog     TEXT   NOT NULL,
    namespace   TEXT   NOT NULL,
    table_name  TEXT   NOT NULL,
    last_status TEXT   NOT NULL CHECK (last_status IN ('green', 'amber', 'red', 'grey')),
    checked_at  BIGINT NOT NULL,
    PRIMARY KEY (catalog, namespace, table_name)
);
