-- The shape of a statement, so repeated runs of one query group together (stardelt#38).
-- sha256 of the SQL with every string literal and every whitespace run normalised; NULL on
-- rows written before this migration. The log keeps 90 days, so the column fills itself.
ALTER TABLE query_log ADD COLUMN sql_fingerprint text;
CREATE INDEX query_log_principal_fingerprint_idx ON query_log (principal, sql_fingerprint)
    WHERE sql_fingerprint IS NOT NULL;
