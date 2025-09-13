CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_weekly_template_7d_pages_overrides` AS
WITH base AS (
  SELECT
    b.username_std, b.page_type, b.username_page, b.scheduler_name,
    b.tz, b.date_local, b.slot_rank, b.hod_local, b.price_usd,
    b.planned_local_datetime, b.scheduled_datetime_utc, b.tracking_hash
  FROM `of-scheduler-proj.mart.v_weekly_template_7d_pages` b
),
r AS (
  SELECT alias_norm, resolved_username_std
  FROM `of-scheduler-proj.layer_04_semantic.v_username_canonical`
)
SELECT
  COALESCE(r.resolved_username_std, base.username_std) AS username_std,
  base.page_type,
  CONCAT(COALESCE(r.resolved_username_std, base.username_std), '__', base.page_type) AS username_page,
  COALESCE(o.assigned_scheduler, base.scheduler_name, 'unassigned') AS scheduler_name,
  base.tz, base.date_local, base.slot_rank, base.hod_local, base.price_usd,
  base.planned_local_datetime, base.scheduled_datetime_utc, base.tracking_hash
FROM base
LEFT JOIN r
  ON r.alias_norm = `of-scheduler-proj.util.norm_username`(base.username_std)
LEFT JOIN `of-scheduler-proj.ops_config.page_scheduler_overrides` o
  ON o.username_std = COALESCE(r.resolved_username_std, base.username_std)
