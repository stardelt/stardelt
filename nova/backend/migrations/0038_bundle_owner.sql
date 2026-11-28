-- Which group a bundle name belongs to (stardelt/stardelt#2230, option b): the first group that
-- recorded a deploy of it or imported a dashboard for it. Another group is refused.
CREATE TABLE bundle_owner (
    bundle TEXT PRIMARY KEY CHECK (char_length(bundle) BETWEEN 1 AND 64),
    owner_group TEXT NOT NULL CHECK (char_length(owner_group) BETWEEN 1 AND 64),
    claimed_by TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- The writers trim a name before they store it (`bundle_owner::claim`), so the backfill trims too:
-- a legacy row stored with surrounding spaces must not become a second, unclaimed name. A row whose
-- name trims to nothing (`deployments::valid_name` allows one) cannot be a claim and is left out.
INSERT INTO bundle_owner (bundle, owner_group, claimed_by, claimed_at)
SELECT DISTINCT ON (trimmed_bundle) trimmed_bundle, trimmed_group, actor, created_at
FROM (
    SELECT BTRIM(bundle, E' \t\n\r\f\v') AS trimmed_bundle,
           BTRIM(owner_group, E' \t\n\r\f\v') AS trimmed_group,
           actor, created_at, id
    FROM deployment
) AS names
WHERE trimmed_bundle <> '' AND trimmed_group <> ''
ORDER BY trimmed_bundle, created_at ASC, id ASC;
