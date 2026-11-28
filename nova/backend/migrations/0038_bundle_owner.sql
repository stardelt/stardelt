-- Which group a bundle name belongs to (stardelt/stardelt#2230, option b): the first group that
-- recorded a deploy of it or imported a dashboard for it. Another group is refused.
CREATE TABLE bundle_owner (
    bundle TEXT PRIMARY KEY CHECK (char_length(bundle) BETWEEN 1 AND 64),
    owner_group TEXT NOT NULL CHECK (char_length(owner_group) BETWEEN 1 AND 64),
    claimed_by TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- TRIM as the writers do (`bundle_owner::claim`): a row stored with surrounding spaces must not
-- become a second, unclaimed name.
INSERT INTO bundle_owner (bundle, owner_group, claimed_by, claimed_at)
SELECT DISTINCT ON (TRIM(bundle)) TRIM(bundle), TRIM(owner_group), actor, created_at
FROM deployment
ORDER BY TRIM(bundle), created_at ASC, id ASC;
