-- Viewers on a notification destination (#79 point 5): groups that may see it, read-only.
-- The owner group still sends the test message, changes and deletes it.
ALTER TABLE destination ADD COLUMN viewers TEXT[] NOT NULL DEFAULT '{}';
CREATE INDEX destination_viewers_idx ON destination USING GIN (viewers);
