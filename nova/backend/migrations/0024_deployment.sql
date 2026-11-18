-- Deployment records (design nova#357 slice 1): what one bundle deploy put on a stage,
-- from which commit, by whom. The pipelines keep their own stamp; this is the history.
CREATE TABLE deployment (
    id BIGSERIAL PRIMARY KEY,
    bundle TEXT NOT NULL CHECK (char_length(bundle) BETWEEN 1 AND 64),
    owner_group TEXT NOT NULL CHECK (char_length(owner_group) BETWEEN 1 AND 64),
    target TEXT NOT NULL CHECK (char_length(target) BETWEEN 1 AND 64),
    revision TEXT,
    repository TEXT,
    status TEXT NOT NULL CHECK (status IN ('applied', 'failed')),
    pipelines JSONB NOT NULL DEFAULT '[]'::jsonb,
    actor TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX deployment_lookup_idx ON deployment (owner_group, bundle, target, created_at DESC);
