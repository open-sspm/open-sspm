-- Final cleanup after account/identity graph cutover.

ALTER TABLE okta_user_groups
  DROP COLUMN IF EXISTS idp_user_id;

ALTER TABLE okta_user_app_assignments
  DROP COLUMN IF EXISTS idp_user_id;

DROP TABLE IF EXISTS identity_links;
DROP TABLE IF EXISTS idp_users;
