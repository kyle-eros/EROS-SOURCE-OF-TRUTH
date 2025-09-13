CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_username_canonical`
OPTIONS(description="Refactored view to provide the canonical list of standardized usernames from the creator dimension.")
AS
SELECT DISTINCT
  username AS username_std
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE;