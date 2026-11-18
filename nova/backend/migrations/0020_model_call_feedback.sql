-- One 👍 or 👎 per recorded model call (design nova#891, slice b): the panel writes it,
-- the model check reads it. Retention is the row's own (NOVA_MODEL_CALL_RETENTION_DAYS).
ALTER TABLE model_call
    ADD COLUMN feedback TEXT,
    ADD CONSTRAINT model_call_feedback_check CHECK (feedback IN ('up', 'down'));
