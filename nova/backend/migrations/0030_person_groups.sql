-- The groups the token carried, so the usage answer can add up by group (usage by group, one
-- column). Written best effort by the person directory from the verified token's `groups`
-- claim; it is a copy of what the identity provider said at the last write, not an authority:
-- authorization still reads the token, never this column. `DEFAULT '{}'` keeps every existing
-- row and every insert that names no groups valid. A person the directory does not know, or a
-- service account such as `superset`, has no row and therefore no group.
-- NEVER edit this file after it has been merged: add 0031_*.sql instead.
ALTER TABLE person ADD COLUMN groups text[] NOT NULL DEFAULT '{}';
