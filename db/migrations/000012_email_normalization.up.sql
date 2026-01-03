-- Normalize stored emails to lower(trim(email)) for consistent matching and indexes.

UPDATE idp_users
SET email = lower(trim(email))
WHERE email <> ''
  AND email <> lower(trim(email));

UPDATE app_users
SET email = lower(trim(email))
WHERE email <> ''
  AND email <> lower(trim(email));

