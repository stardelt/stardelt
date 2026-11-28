-- A person keeps a conversation by pinning it; pinned rows are never pruned (maintainer, 2026-11-28).
ALTER TABLE ask_conversation ADD COLUMN pinned BOOLEAN NOT NULL DEFAULT false;
