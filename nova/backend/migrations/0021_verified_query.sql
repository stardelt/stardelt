-- Verified queries (design nova#911 slice 2a): a question, the SQL that answers it, who verified it.
CREATE TABLE verified_query (
    id BIGSERIAL PRIMARY KEY,
    catalog TEXT NOT NULL,
    namespace TEXT NOT NULL,
    table_name TEXT NOT NULL,
    question TEXT NOT NULL CHECK (char_length(question) BETWEEN 1 AND 500),
    sql TEXT NOT NULL CHECK (char_length(sql) BETWEEN 1 AND 20000),
    verified_by TEXT NOT NULL,
    verified_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX verified_query_table_idx ON verified_query (catalog, namespace, table_name);
