CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_pages`
OPTIONS(description="Refactored view to provide the definitive list of creator pages and their types.")
AS
SELECT
  username AS username_std,
  account_type AS page_type,
  CONCAT(username, '__', account_type) AS username_page,
  last_active_date AS decided_as_of
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE;