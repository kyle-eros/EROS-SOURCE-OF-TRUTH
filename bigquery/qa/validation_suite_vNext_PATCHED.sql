-- =====================================================
-- QA VALIDATION SUITE v.Next [PATCHED]
-- =====================================================
-- Fixes: Diversity computed on canonical ladder row,
-- cooldown timestamp math, test runner dry_run flag
-- =====================================================

-- 1. CAPTION DIVERSITY CHECK [FIXED: Filter to canonical ladder position]
CREATE OR REPLACE VIEW `of-scheduler-proj.qa.caption_diversity_check_vNext` AS
WITH diversity_metrics AS (
  SELECT
    username_page,
    schedule_date,
    COUNT(DISTINCT caption_id) AS unique_captions,
    COUNT(*) AS total_slots,
    SAFE_DIVIDE(COUNT(DISTINCT caption_id), COUNT(*)) AS diversity_ratio,
    COUNT(DISTINCT category) AS unique_categories,
    MAX(caption_count) AS max_caption_repeats
  FROM (
    SELECT
      username_page,
      DATE(schedule_date) AS schedule_date,
      caption_id,
      category,
      COUNT(*) AS caption_count
    FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`
    WHERE schedule_date >= CURRENT_DATE()
      AND schedule_date < DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
      AND `Ladder Position` = 3  -- FIXED: Only count canonical position
    GROUP BY username_page, schedule_date, caption_id, category
  )
  GROUP BY username_page, schedule_date
)
SELECT
  username_page,
  schedule_date,
  unique_captions,
  total_slots,
  ROUND(diversity_ratio, 3) AS diversity_ratio,
  unique_categories,
  max_caption_repeats,
  CASE 
    WHEN diversity_ratio < 0.85 THEN 'FAIL'
    WHEN diversity_ratio < 0.90 THEN 'WARN'
    ELSE 'PASS'
  END AS diversity_status,
  CASE
    WHEN max_caption_repeats > 5 THEN 'FAIL: Caption repeated >5 times'
    WHEN diversity_ratio < 0.85 THEN 'FAIL: Diversity < 85%'
    WHEN diversity_ratio < 0.90 THEN 'WARN: Diversity < 90%'
    ELSE 'OK'
  END AS validation_message,
  CURRENT_TIMESTAMP() AS checked_at
FROM diversity_metrics;

-- 2. COOLDOWN VIOLATIONS CHECK [FIXED: TIMESTAMP_DIFF]
CREATE OR REPLACE VIEW `of-scheduler-proj.qa.cooldown_violations_vNext` AS
WITH scheduled_captions AS (
  SELECT
    username_page,
    caption_id,
    schedule_date,
    hod_local,
    -- Convert to timestamp for proper diff calculation
    TIMESTAMP(DATETIME(schedule_date, TIME(hod_local, 0, 0))) AS slot_timestamp,
    LAG(TIMESTAMP(DATETIME(schedule_date, TIME(hod_local, 0, 0)))) OVER (
      PARTITION BY username_page, caption_id 
      ORDER BY schedule_date, hod_local
    ) AS prev_slot_timestamp
  FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`
  WHERE schedule_date >= CURRENT_DATE()
    AND schedule_date < DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
    AND caption_id NOT LIKE 'FALLBACK%'
    AND `Ladder Position` = 3  -- FIXED: Only check canonical position
),
violations AS (
  SELECT
    username_page,
    caption_id,
    slot_timestamp,
    prev_slot_timestamp,
    TIMESTAMP_DIFF(slot_timestamp, prev_slot_timestamp, DAY) AS days_between,
    TIMESTAMP_DIFF(slot_timestamp, prev_slot_timestamp, HOUR) AS hours_between
  FROM scheduled_captions
  WHERE prev_slot_timestamp IS NOT NULL
)
SELECT
  username_page,
  COUNT(*) AS total_scheduled,
  COUNT(CASE WHEN days_between < 7 THEN 1 END) AS violations_7d,
  COUNT(CASE WHEN hours_between < 168 THEN 1 END) AS violations_168h,
  ROUND(SAFE_DIVIDE(
    COUNT(CASE WHEN hours_between < 168 THEN 1 END),
    COUNT(*)
  ), 4) AS violation_rate,
  CASE
    WHEN COUNT(CASE WHEN hours_between < 168 THEN 1 END) = 0 THEN 'PASS'
    WHEN SAFE_DIVIDE(COUNT(CASE WHEN hours_between < 168 THEN 1 END), COUNT(*)) > 0.01 THEN 'FAIL'
    ELSE 'WARN'
  END AS cooldown_status,
  STRING_AGG(
    CASE 
      WHEN hours_between < 168 THEN 
        CONCAT(caption_id, ' (', hours_between, ' hours)')
    END, ', ' 
    ORDER BY hours_between
    LIMIT 5
  ) AS sample_violations,
  CURRENT_TIMESTAMP() AS checked_at
FROM violations
GROUP BY username_page;

-- 3. SCORING DISTRIBUTION CHECK [FIXED: Filter canonical position]
CREATE OR REPLACE VIEW `of-scheduler-proj.qa.scoring_distribution_vNext` AS
SELECT
  username_page,
  COUNT(*) AS total_scores,
  MIN(score_final) AS min_score,
  MAX(score_final) AS max_score,
  AVG(score_final) AS avg_score,
  STDDEV(score_final) AS stddev_score,
  APPROX_QUANTILES(score_final, 4) AS score_quartiles,
  COUNT(CASE WHEN score_final = 0 THEN 1 END) AS zero_scores,
  COUNT(CASE WHEN score_final > 100 THEN 1 END) AS over_100_scores,
  COUNT(CASE WHEN score_final < 0 THEN 1 END) AS negative_scores,
  CASE
    WHEN COUNT(CASE WHEN score_final < 0 OR score_final > 100 THEN 1 END) > 0 THEN 'FAIL'
    WHEN STDDEV(score_final) < 5 THEN 'WARN: Low variation'
    WHEN COUNT(CASE WHEN score_final = 0 THEN 1 END) > total_scores * 0.1 THEN 'WARN: Many zeros'
    ELSE 'PASS'
  END AS score_status,
  CURRENT_TIMESTAMP() AS checked_at
FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`
WHERE schedule_date >= CURRENT_DATE()
  AND schedule_date < DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
  AND `Ladder Position` = 3  -- FIXED: Only check canonical position
GROUP BY username_page;

-- 4. DATA COMPLETENESS CHECK [FIXED: Check canonical position]
CREATE OR REPLACE VIEW `of-scheduler-proj.qa.data_completeness_vNext` AS
SELECT
  DATE(schedule_date) AS schedule_date,
  COUNT(DISTINCT username_page) AS pages_with_schedule,
  COUNT(*) AS total_rows,
  COUNT(caption_id) AS rows_with_caption_id,
  COUNT(caption_text) AS rows_with_caption_text,
  COUNT(recommended_price) AS rows_with_price,
  COUNT(CASE WHEN caption_id IS NULL THEN 1 END) AS null_caption_ids,
  COUNT(CASE WHEN caption_id LIKE 'FALLBACK%' THEN 1 END) AS fallback_captions,
  ROUND(SAFE_DIVIDE(COUNT(caption_id), COUNT(*)), 4) AS caption_fill_rate,
  ROUND(SAFE_DIVIDE(
    COUNT(CASE WHEN caption_id LIKE 'FALLBACK%' THEN 1 END),
    COUNT(*)
  ), 4) AS fallback_rate,
  CASE
    WHEN COUNT(caption_id) < COUNT(*) * 0.999 THEN 'FAIL: Missing captions'
    WHEN SAFE_DIVIDE(COUNT(CASE WHEN caption_id LIKE 'FALLBACK%' THEN 1 END), COUNT(*)) > 0.03 THEN 'WARN: High fallback rate'
    ELSE 'PASS'
  END AS completeness_status,
  CURRENT_TIMESTAMP() AS checked_at
FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`
WHERE schedule_date >= CURRENT_DATE()
  AND schedule_date < DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
  AND `Ladder Position` = 3  -- FIXED: Only check canonical position
GROUP BY schedule_date;

-- 5. DAILY HEALTH SUMMARY [Uses fixed views above]
CREATE OR REPLACE VIEW `of-scheduler-proj.qa.daily_health_vNext` AS
WITH metrics AS (
  SELECT
    'diversity' AS metric_name,
    AVG(diversity_ratio) AS metric_value,
    MIN(CASE WHEN diversity_status = 'FAIL' THEN 0 ELSE 1 END) AS all_pass
  FROM `of-scheduler-proj.qa.caption_diversity_check_vNext`
  
  UNION ALL
  
  SELECT
    'cooldown_compliance',
    1 - AVG(violation_rate),
    MIN(CASE WHEN cooldown_status = 'FAIL' THEN 0 ELSE 1 END)
  FROM `of-scheduler-proj.qa.cooldown_violations_vNext`
  
  UNION ALL
  
  SELECT
    'score_quality',
    AVG(CASE WHEN score_status = 'PASS' THEN 1 ELSE 0 END),
    MIN(CASE WHEN score_status = 'FAIL' THEN 0 ELSE 1 END)
  FROM `of-scheduler-proj.qa.scoring_distribution_vNext`
  
  UNION ALL
  
  SELECT
    'data_completeness',
    AVG(caption_fill_rate),
    MIN(CASE WHEN completeness_status = 'FAIL' THEN 0 ELSE 1 END)
  FROM `of-scheduler-proj.qa.data_completeness_vNext`
),
thresholds AS (
  SELECT
    metric_name,
    min_threshold,
    max_threshold,
    alert_enabled,
    auto_rollback_enabled
  FROM `of-scheduler-proj.ops.quality_thresholds_v1`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY metric_name ORDER BY updated_at DESC) = 1
)
SELECT
  m.metric_name,
  ROUND(m.metric_value, 4) AS metric_value,
  t.min_threshold,
  t.max_threshold,
  CASE
    WHEN t.min_threshold IS NOT NULL AND m.metric_value < t.min_threshold THEN 'FAIL: Below minimum'
    WHEN t.max_threshold IS NOT NULL AND m.metric_value > t.max_threshold THEN 'FAIL: Above maximum'
    WHEN m.all_pass = 0 THEN 'FAIL: Some pages failed'
    ELSE 'PASS'
  END AS status,
  t.alert_enabled,
  t.auto_rollback_enabled,
  CASE
    WHEN t.auto_rollback_enabled 
      AND ((t.min_threshold IS NOT NULL AND m.metric_value < t.min_threshold)
           OR (t.max_threshold IS NOT NULL AND m.metric_value > t.max_threshold))
    THEN TRUE
    ELSE FALSE
  END AS trigger_rollback,
  CURRENT_TIMESTAMP() AS evaluated_at
FROM metrics m
LEFT JOIN thresholds t USING (metric_name);

-- 6. COST TRACKING [No changes needed]
CREATE OR REPLACE VIEW `of-scheduler-proj.qa.cost_tracking_vNext` AS
SELECT
  CURRENT_DATE() AS date,
  'caption_features_vNext' AS view_name,
  1073741824 AS estimated_bytes,
  1073741824 / 1099511627776.0 * 6.25 AS estimated_cost_usd
UNION ALL
SELECT
  CURRENT_DATE(),
  'caption_ranker_vNext',
  2147483648,
  2147483648 / 1099511627776.0 * 6.25
UNION ALL
SELECT
  CURRENT_DATE(),
  'sheets_schedule_export_vNext',
  536870912,
  536870912 / 1099511627776.0 * 6.25;