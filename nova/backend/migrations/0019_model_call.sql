-- One row per call Nova makes to a model endpoint (design nova#890, slice 1).
-- Sizes and hashes only: no prompt and no answer text, so a person leaving needs no deletion.
-- Pruned hourly, 30 days by default (NOVA_MODEL_CALL_RETENTION_DAYS=0 switches it off).
CREATE TABLE model_call (
    id BIGSERIAL PRIMARY KEY,
    at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sub TEXT NOT NULL,
    group_name TEXT,
    feature TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    model TEXT NOT NULL,
    tokens_in BIGINT NOT NULL,
    tokens_out BIGINT NOT NULL,
    tokens_estimated BOOLEAN NOT NULL,
    latency_ms BIGINT NOT NULL,
    outcome TEXT NOT NULL,
    prompt_bytes BIGINT NOT NULL,
    prompt_sha256 TEXT NOT NULL,
    answer_bytes BIGINT,
    answer_sha256 TEXT,
    CONSTRAINT model_call_feature_check CHECK (feature IN ('ask', 'agent', 'probe')),
    CONSTRAINT model_call_outcome_check CHECK (outcome IN ('ok', 'refused', 'error'))
);
CREATE INDEX model_call_at_idx ON model_call (at DESC);
CREATE INDEX model_call_group_at_idx ON model_call (group_name, at DESC);
