-- Eval definitions (stardelt/stardelt#125, slice 1): a question with a known answer, kept
-- next to the catalog's other context. Stored, never executed, by this table.
CREATE TABLE eval (
    id BIGSERIAL PRIMARY KEY,
    catalog TEXT NOT NULL,
    owner_group TEXT NOT NULL,
    question TEXT NOT NULL CHECK (char_length(question) BETWEEN 1 AND 500),
    expected JSONB NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX eval_catalog_idx ON eval (catalog, id);
