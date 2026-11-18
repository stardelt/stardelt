-- Expected results and the last check of a verified query (design nova#912 slice 1).
ALTER TABLE verified_query
    ADD COLUMN expected JSONB,
    ADD COLUMN last_check_at TIMESTAMPTZ,
    ADD COLUMN last_check_passed BOOLEAN,
    ADD COLUMN last_check_message TEXT;
