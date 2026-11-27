-- Eval runs (stardelt/stardelt#125, slice 2): one posted answer, its verdict and the model that
-- wrote it. The eval row owns the question; this table is its history.
CREATE TABLE eval_run (
    id BIGSERIAL PRIMARY KEY,
    eval_id BIGINT NOT NULL REFERENCES eval(id) ON DELETE CASCADE,
    passed BOOLEAN NOT NULL,
    reason TEXT,
    sql TEXT,
    model TEXT,
    endpoint TEXT,
    tokens BIGINT,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX eval_run_eval_idx ON eval_run (eval_id, id DESC);
