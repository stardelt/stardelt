-- The files whose content one deploy delivered (stardelt/stardelt#102): one path and one sha256
-- per file under the bundle's `include:` and `files:`, so a record says what built its documents.
ALTER TABLE deployment ADD COLUMN files JSONB NOT NULL DEFAULT '[]'::jsonb;
