CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_dim`
OPTIONS(description="Refactored page dimension, sourcing all attributes from the new centralized creator dimension.")
AS
SELECT
  c.username AS username_std,
  COALESCE(ovr.assigned_scheduler, c.scheduler_info.scheduler_email, 'unassigned') AS assigned_scheduler,
  COALESCE(t.recommended_tier, c.performance_metrics.performance_segment) AS tier,
  -- Placeholder for TZ info, which needs a new source table
  'UTC' AS tz,
  NULL AS min_hod,
  NULL AS max_hod,
  TRUE AS is_active
FROM `of-scheduler-proj.layer_04_semantic.v_creator_active_current` a
JOIN `of-scheduler-proj.layer_03_foundation.dim_creator` c
  ON a.username_std = c.username AND c.is_current_record = TRUE
LEFT JOIN `of-scheduler-proj.ops_config.v_scheduler_overrides` ovr
  ON a.username_std = ovr.username_std
LEFT JOIN `of-scheduler-proj.layer_04_semantic.v_creator_tier_assignment` t
  ON c.creator_key = t.creator_key;