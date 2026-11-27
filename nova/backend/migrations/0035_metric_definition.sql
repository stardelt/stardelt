-- Metric definitions (stardelt/stardelt#124, fourth context kind): a named number over a
-- table, the SQL expression that computes it, an optional time column and the dimensions it
-- may be cut by. A definition is stored, never executed, by this table.
CREATE TABLE metric_definition (
    id BIGSERIAL PRIMARY KEY,
    catalog TEXT NOT NULL,
    namespace TEXT NOT NULL,
    table_name TEXT NOT NULL,
    name TEXT NOT NULL CHECK (name ~ '^[a-z][a-z0-9_]{0,63}$'),
    description TEXT NOT NULL CHECK (char_length(description) BETWEEN 1 AND 500),
    expression TEXT NOT NULL CHECK (char_length(expression) BETWEEN 1 AND 2000),
    time_column TEXT,
    dimensions TEXT[] NOT NULL DEFAULT '{}',
    verified_by TEXT NOT NULL,
    verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (catalog, namespace, table_name, name)
);
CREATE INDEX metric_definition_table_idx ON metric_definition (catalog, namespace, table_name);
