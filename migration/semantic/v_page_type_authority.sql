CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_type_authority`
OPTIONS(description="Refactored view to determine the authoritative page type (e.g., 'vip', 'main') from the creator dimension.")
AS
SELECT
  username AS username_std,
  account_type AS page_type,
  -- The date of the last classification would be ideal here.
  -- Using last_active_date as a proxy.
  last_active_date AS decided_as_of
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE;