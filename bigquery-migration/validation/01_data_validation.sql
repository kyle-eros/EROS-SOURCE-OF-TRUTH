-- =========================================
-- DATA VALIDATION QUERIES
-- =========================================
-- Purpose: Validate data integrity after migration
-- Compare old vs new architecture
-- =========================================

-- 1. Check row counts between old and new
WITH comparison AS (
  SELECT
    'Old System' AS system,
    COUNT(DISTINCT caption_hash) AS caption_count,
    COUNT(DISTINCT username_std) AS creator_count,
    COUNT(*) AS message_send_count
  FROM `of-scheduler-proj.raw.message_facts`
  WHERE DATE(sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  
  UNION ALL
  
  SELECT
    'New System' AS system,
    COUNT(DISTINCT caption_id) AS caption_count,
    COUNT(DISTINCT creator_key) AS creator_count,
    COUNT(*) AS message_send_count
  FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
  WHERE send_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)
SELECT
  system,
  caption_count,
  creator_count,
  message_send_count
FROM comparison;

-- 2. Validate caption ID mapping
SELECT
  'Caption Mapping Check' AS check_type,
  COUNT(*) AS total_captions,
  COUNTIF(caption_id IS NOT NULL) AS mapped_captions,
  COUNTIF(caption_id IS NULL) AS unmapped_captions,
  ROUND(COUNTIF(caption_id IS NOT NULL) / COUNT(*) * 100, 2) AS mapping_rate_pct
FROM (
  SELECT DISTINCT
    caption_hash,
    CONCAT('CAP_', caption_hash) AS caption_id
  FROM `of-scheduler-proj.raw.caption_uploads`
) c
LEFT JOIN `of-scheduler-proj.layer_03_foundation.dim_caption` dc
  ON c.caption_id = dc.caption_id;

-- 3. Check data quality metrics
SELECT
  'Data Quality' AS check_type,
  
  -- Check for nulls in critical fields
  COUNTIF(caption_key = 'UNKNOWN') AS unknown_captions,
  COUNTIF(creator_key = 'UNKNOWN') AS unknown_creators,
  
  -- Check for data anomalies
  COUNTIF(messages_purchased > messages_sent) AS purchase_anomalies,
  COUNTIF(messages_viewed > messages_sent) AS view_anomalies,
  COUNTIF(gross_revenue_usd < 0) AS negative_revenue,
  
  -- Quality flag distribution
  COUNTIF(quality_flag = 'valid') AS valid_records,
  COUNTIF(quality_flag != 'valid') AS invalid_records,
  
  ROUND(COUNTIF(quality_flag = 'valid') / COUNT(*) * 100, 2) AS quality_rate_pct
  
FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
WHERE send_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- 4. Validate ML feature calculations
WITH feature_validation AS (
  SELECT
    username_page,
    COUNT(*) AS caption_count,
    AVG(performance_features.rps_smoothed) AS avg_rps,
    AVG(performance_features.confidence_score) AS avg_confidence,
    COUNTIF(cooldown_features.is_eligible) AS eligible_count,
    ROUND(COUNTIF(cooldown_features.is_eligible) / COUNT(*) * 100, 2) AS eligibility_rate_pct
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date = CURRENT_DATE()
  GROUP BY username_page
)
SELECT
  'ML Features' AS check_type,
  COUNT(DISTINCT username_page) AS total_pages,
  AVG(caption_count) AS avg_captions_per_page,
  AVG(avg_rps) AS overall_avg_rps,
  AVG(avg_confidence) AS overall_avg_confidence,
  AVG(eligibility_rate_pct) AS avg_eligibility_rate_pct
FROM feature_validation;

-- 5. Check export layer completeness
SELECT
  'Export Layer' AS check_type,
  COUNT(DISTINCT username_page) AS pages_with_recommendations,
  COUNT(DISTINCT caption_id) AS unique_captions_recommended,
  AVG(recommendation_rank) AS avg_rank,
  AVG(ml_score) AS avg_ml_score,
  AVG(predicted_rps) AS avg_predicted_rps
FROM `of-scheduler-proj.layer_07_export.schedule_recommendations`
WHERE schedule_date = CURRENT_DATE();

-- 6. Verify configuration is loaded
SELECT
  'Configuration' AS check_type,
  COUNT(*) AS config_count,
  COUNTIF(is_active) AS active_configs,
  STRING_AGG(DISTINCT page_state) AS page_states_configured,
  STRING_AGG(DISTINCT environment) AS environments
FROM `of-scheduler-proj.ops_config.ml_parameters`;

-- 7. Summary health check
WITH health_metrics AS (
  SELECT
    (SELECT COUNT(*) FROM `of-scheduler-proj.layer_03_foundation.dim_caption`) AS captions,
    (SELECT COUNT(*) FROM `of-scheduler-proj.layer_03_foundation.dim_creator`) AS creators,
    (SELECT COUNT(*) FROM `of-scheduler-proj.layer_03_foundation.fact_message_send` 
     WHERE send_date = CURRENT_DATE()) AS sends_today,
    (SELECT COUNT(*) FROM `of-scheduler-proj.layer_05_ml.feature_store` 
     WHERE computed_date = CURRENT_DATE()) AS features_computed,
    (SELECT COUNT(*) FROM `of-scheduler-proj.layer_07_export.schedule_recommendations` 
     WHERE schedule_date = CURRENT_DATE()) AS recommendations_generated
)
SELECT
  'System Health' AS check_type,
  CASE
    WHEN captions > 0 
     AND creators > 0 
     AND features_computed > 0 
     AND recommendations_generated > 0
    THEN '✓ HEALTHY'
    ELSE '✗ ISSUES DETECTED'
  END AS status,
  captions,
  creators,
  sends_today,
  features_computed,
  recommendations_generated
FROM health_metrics;