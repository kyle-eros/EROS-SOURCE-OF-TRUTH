INSERT INTO `of-scheduler-proj.mart.weekly_template_qc_violations`
(run_at, issue_type, username_std, date_local, prev_h, curr_h, next_h, gap_h, min_h, max_h)
WITH plan AS (
  SELECT username_std, date_local, CAST(hod_local AS INT64) AS h
  FROM `of-scheduler-proj.mart.weekly_template_7d_latest`
),
win AS (
  SELECT
    username_std,
    CAST(COALESCE(MIN(min_hod), 0)  AS INT64) AS min_h,
    CAST(COALESCE(MAX(max_hod), 23) AS INT64) AS max_h
  FROM `of-scheduler-proj.layer_04_semantic.v_page_dim`
  WHERE COALESCE(LOWER(CAST(is_active AS STRING)) IN ('true','t','1','yes','y'), TRUE)
  GROUP BY username_std
),
ordered AS (
  SELECT
    p.username_std,
    p.date_local,
    p.h,
    w.min_h,
    w.max_h,
    LAG(p.h)  OVER (PARTITION BY p.username_std, p.date_local ORDER BY p.h) AS prev_h,
    LEAD(p.h) OVER (PARTITION BY p.username_std, p.date_local ORDER BY p.h) AS next_h
  FROM plan p
  JOIN win  w USING (username_std)
),
violations AS (
  SELECT 'MIN_GAP_LT_2H' AS issue_type, username_std, date_local,
         prev_h, h AS curr_h, next_h, (h - prev_h) AS gap_h, min_h, max_h
  FROM ordered
  WHERE prev_h IS NOT NULL AND (h - prev_h) < 2
  UNION ALL
  SELECT 'MAX_GAP_GT_6H_IN_WINDOW', username_std, date_local,
         NULL AS prev_h, h AS curr_h, next_h, (next_h - h) AS gap_h, min_h, max_h
  FROM ordered
  WHERE next_h IS NOT NULL AND (next_h - h) > 6 AND h BETWEEN min_h AND max_h
)
SELECT CURRENT_TIMESTAMP(), issue_type, username_std, date_local,
       prev_h, curr_h, next_h, gap_h, min_h, max_h
FROM violations;
