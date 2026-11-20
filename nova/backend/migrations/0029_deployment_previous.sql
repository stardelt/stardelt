-- The deployment a rollback replayed (stardelt/stardelt#102): a chain a person can follow from the replay back to
-- what it restored. NULL for a deploy that was not a rollback.
ALTER TABLE deployment ADD COLUMN previous_deployment_id BIGINT REFERENCES deployment(id) ON DELETE SET NULL;
