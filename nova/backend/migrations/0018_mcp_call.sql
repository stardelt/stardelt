-- One row per /mcp tool call, for the administrator's audit view (design agent-access.md §4, T7).
-- The tracing line stays; this is the queryable copy. Pruned hourly, 30 days by default.
CREATE TABLE mcp_call (
    id BIGSERIAL PRIMARY KEY,
    at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sub TEXT NOT NULL,
    client TEXT,
    tool TEXT NOT NULL,
    status TEXT NOT NULL,
    duration_ms BIGINT NOT NULL,
    arguments_sha256 TEXT NOT NULL
);
CREATE INDEX mcp_call_at_idx ON mcp_call (at DESC);
CREATE INDEX mcp_call_sub_at_idx ON mcp_call (sub, at DESC);
