WITH base AS (
  SELECT * FROM `of-scheduler-proj.mart.weekly_template_7d_latest`
),
types AS (
  SELECT username_std, page_type FROM `of-scheduler-proj.core.v_pages`
),
assign AS (
  SELECT username_std, ANY_VALUE(assigned_scheduler) AS assigned_scheduler
  FROM `of-scheduler-proj.core.page_dim`
  WHERE COALESCE(is_active, TRUE)
  GROUP BY username_std
)
SELECT
  b.username_std,
  t.page_type,
  CONCAT(b.username_std,'__',t.page_type) AS username_page,
  COALESCE(a.assigned_scheduler, b.scheduler_name, 'unassigned') AS scheduler_name,
  b.tz, b.date_local, b.slot_rank, b.hod_local, b.price_usd,
  b.planned_local_datetime, b.scheduled_datetime_utc,
  TO_BASE64(SHA256(CONCAT(
    b.username_std,'__',t.page_type,'|',CAST(b.date_local AS STRING),'|',CAST(b.hod_local AS STRING)
  ))) AS tracking_hash
FROM base b
JOIN types t USING (username_std)
LEFT JOIN assign a USING (username_std)
