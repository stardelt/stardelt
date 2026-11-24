-- One person's request to read or write a catalog, approved by its owner group (D12, #489).
-- Slice 1 stores the request; the grant is applied by the approval slice, never here.
CREATE TABLE access_request (
    id BIGSERIAL PRIMARY KEY,
    requester_sub TEXT NOT NULL,
    requester_name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    catalog TEXT NOT NULL,
    verbs TEXT[] NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'withdrawn')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    state_changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX access_request_by_requester ON access_request (requester_sub, created_at DESC);
