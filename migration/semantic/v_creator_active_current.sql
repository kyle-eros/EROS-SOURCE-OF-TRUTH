CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_creator_active_current`
OPTIONS(description="Refactored view to identify currently active creators based on recent activity in the creator dimension.")
AS
SELECT
  username AS username_std,
  scheduler_info.scheduler_email AS assigned_scheduler,
  performance_metrics.performance_segment AS tier,
  TRUE AS is_active
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE
  AND account_status = 'active'
  AND last_active_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 21 DAY);