CREATE OR REPLACE VIEW `of-scheduler-proj.ops_config.v_scheduler_overrides`
OPTIONS(description="Refactored view to show current scheduler assignments directly from the creator dimension.")
AS
SELECT
  username AS username_std,
  scheduler_info.scheduler_email AS assigned_scheduler,
  -- NOTE: assignment_date is a DATE, original updated_at was TIMESTAMP. Coercing for compatibility.
  CAST(scheduler_info.assignment_date AS TIMESTAMP) AS updated_at
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE
  AND scheduler_info.scheduler_email IS NOT NULL;