-- The rendered Pipeline documents of one deploy (stardelt/stardelt#102 slice 1):
-- exactly what was applied, so a rollback replays the record, not Git.
ALTER TABLE deployment ADD COLUMN resources JSONB NOT NULL DEFAULT '[]'::jsonb;
