-- The person's name beside their subject (design nova#357): an OIDC `sub` is a UUID on
-- Keycloak, and the Deployments page must name the person, not the number.
ALTER TABLE deployment ADD COLUMN actor_name TEXT;
