-- The last 20 questions a person asked Ask (design nova#891 slice c): one row per answer.
CREATE TABLE ask_conversation (
    id BIGSERIAL PRIMARY KEY,
    sub TEXT NOT NULL,
    question TEXT NOT NULL CHECK (char_length(question) BETWEEN 1 AND 2000),
    endpoint TEXT NOT NULL,
    model TEXT,
    call_id BIGINT,
    sql TEXT,
    sentence TEXT NOT NULL,
    tables JSONB NOT NULL DEFAULT '[]'::jsonb,
    explain TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ask_conversation_sub_idx ON ask_conversation (sub, created_at DESC);
