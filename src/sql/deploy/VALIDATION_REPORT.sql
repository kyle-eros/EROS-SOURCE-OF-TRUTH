-- =====================================================
-- EROS ENHANCED SYSTEM - VALIDATION REPORT
-- Comprehensive coverage and completeness analysis
-- =====================================================

-- Coverage validation across key dimensions
WITH coverage_analysis AS (
  -- 1. Caption Classification Coverage
  SELECT 
    'Caption Classification' AS dimension,
    COUNT(*) AS total_records,
    COUNTIF(explicitness IS NOT NULL) AS classified_records,
    SAFE_DIVIDE(COUNTIF(explicitness IS NOT NULL), COUNT(*)) AS coverage_pct,
    ARRAY_AGG(DISTINCT explicitness IGNORE NULLS) AS distinct_values
  FROM `of-scheduler-proj.raw.caption_library`
  
  UNION ALL
  
  -- 2. Content Profile Coverage  
  SELECT 
    'Content Profiles' AS dimension,
    COUNT(DISTINCT username_std) AS total_records,
    COUNT(DISTINCT p.username_std) AS classified_records,
    SAFE_DIVIDE(COUNT(DISTINCT p.username_std), COUNT(DISTINCT pd.username_std)) AS coverage_pct,
    ARRAY_AGG(DISTINCT p.max_explicitness IGNORE NULLS) AS distinct_values
  FROM `of-scheduler-proj.core.page_dim` pd
  LEFT JOIN `of-scheduler-proj.core.page_content_profile_v1` p USING (username_std)
  
  UNION ALL
  
  -- 3. Performance Data Coverage (last 30 days)
  SELECT 
    'Recent Performance' AS dimension,
    COUNT(DISTINCT username_std) AS total_records,
    COUNT(DISTINCT CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN username_std END) AS classified_records,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN username_std END),
      COUNT(DISTINCT username_std)
    ) AS coverage_pct,
    [CAST(COUNTIF(sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS STRING)] AS distinct_values
  FROM `of-scheduler-proj.core.message_facts`
  
  UNION ALL
  
  -- 4. Cooldown System Coverage
  SELECT 
    'Smart Cooldowns' AS dimension,
    COUNT(DISTINCT CONCAT(caption_hash, '|', username_std)) AS total_records,
    COUNT(DISTINCT CASE WHEN available_after IS NOT NULL THEN CONCAT(caption_hash, '|', username_std) END) AS classified_records,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN available_after IS NOT NULL THEN CONCAT(caption_hash, '|', username_std) END),
      COUNT(DISTINCT CONCAT(caption_hash, '|', username_std))
    ) AS coverage_pct,
    ARRAY_AGG(DISTINCT performance_tier IGNORE NULLS LIMIT 5) AS distinct_values
  FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
  
  UNION ALL
  
  -- 5. Sentiment Analysis Coverage
  SELECT 
    'Sentiment Analysis' AS dimension,
    COUNT(DISTINCT caption_hash) AS total_records,
    COUNT(DISTINCT CASE WHEN engagement_prediction_score IS NOT NULL THEN caption_hash END) AS classified_records,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN engagement_prediction_score IS NOT NULL THEN caption_hash END),
      COUNT(DISTINCT caption_hash)
    ) AS coverage_pct,
    [CAST(ROUND(AVG(engagement_prediction_score), 3) AS STRING)] AS distinct_values
  FROM `of-scheduler-proj.core.v_caption_sentiment_v1`
  
  UNION ALL
  
  -- 6. Engagement Patterns Coverage
  SELECT 
    'Engagement Patterns' AS dimension,
    COUNT(DISTINCT username_std) AS total_records,
    COUNT(DISTINCT CASE WHEN schedule_coverage > 0 THEN username_std END) AS classified_records,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN schedule_coverage > 0 THEN username_std END),
      COUNT(DISTINCT username_std)
    ) AS coverage_pct,
    [CAST(ROUND(AVG(schedule_coverage), 3) AS STRING)] AS distinct_values
  FROM `of-scheduler-proj.core.v_page_engagement_patterns_v1`
),

-- Data quality checks
quality_analysis AS (
  -- Check for required fields and data consistency
  SELECT 
    'Data Quality Issues' AS category,
    ARRAY_AGG(STRUCT(
      check_name,
      issue_count,
      total_records,
      SAFE_DIVIDE(issue_count, total_records) AS issue_rate
    )) AS quality_checks
  FROM (
    SELECT 'Missing Caption Text' AS check_name, 
           COUNTIF(caption_text IS NULL OR TRIM(caption_text) = '') AS issue_count,
           COUNT(*) AS total_records
    FROM `of-scheduler-proj.raw.caption_library`
    
    UNION ALL
    
    SELECT 'Invalid Explicitness Values' AS check_name,
           COUNTIF(explicitness NOT IN ('explicit', 'moderate', 'mild', 'gfe-implied', 'other')) AS issue_count,
           COUNT(*) AS total_records  
    FROM `of-scheduler-proj.raw.caption_library`
    WHERE explicitness IS NOT NULL
    
    UNION ALL
    
    SELECT 'Negative Cooldown Days' AS check_name,
           COUNTIF(final_cooldown_days < 0) AS issue_count,
           COUNT(*) AS total_records
    FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
    
    UNION ALL
    
    SELECT 'Future Last Used Dates' AS check_name,
           COUNTIF(last_used_ts > CURRENT_TIMESTAMP()) AS issue_count,
           COUNT(*) AS total_records
    FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
    WHERE last_used_ts IS NOT NULL
    
    UNION ALL
    
    SELECT 'Invalid Performance Tiers' AS check_name,
           COUNTIF(performance_tier NOT IN ('ELITE', 'HIGH_PERFORMER', 'AVERAGE', 'UNTESTED')) AS issue_count,
           COUNT(*) AS total_records
    FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
    WHERE performance_tier IS NOT NULL
  )
),

-- System performance metrics
performance_metrics AS (
  SELECT
    'System Performance' AS category,
    STRUCT(
      -- Query performance tests
      (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_smart_cooldowns_v1` LIMIT 1000) AS cooldown_query_test,
      (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_caption_sentiment_v1` LIMIT 1000) AS sentiment_query_test,
      (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_performance_alerts_v1` WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) LIMIT 1000) AS alerts_query_test,
      
      -- Data freshness checks
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(calculated_at), MINUTE) AS cooldown_freshness_minutes,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(alert_generated_at), MINUTE) AS alerts_freshness_minutes
    ) AS metrics
  FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
  CROSS JOIN `of-scheduler-proj.core.v_performance_alerts_v1`
  WHERE DATE(alert_generated_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

-- Feature utilization analysis
feature_utilization AS (
  SELECT
    'Feature Utilization' AS category,
    STRUCT(
      -- Smart cooldown features
      COUNTIF(cooldown_override_suggested) AS override_suggestions,
      COUNTIF(emergency_override_eligible) AS emergency_overrides,
      COUNTIF(fatigue_score >= 0.5) AS fatigued_captions,
      
      -- Performance monitoring
      COUNT(DISTINCT username_std) AS pages_with_alerts,
      COUNTIF(performance_status IN ('CRITICAL_LOW', 'WARNING')) AS active_alerts,
      
      -- Content safety
      COUNT(DISTINCT p.username_std) AS pages_with_profiles,
      COUNTIF(s.engagement_prediction_score > 0.7) AS high_engagement_captions
    ) AS utilization_stats
  FROM `of-scheduler-proj.core.v_smart_cooldowns_v1` sc
  LEFT JOIN `of-scheduler-proj.core.page_content_profile_v1` p ON p.username_std = REGEXP_EXTRACT(sc.username_std, r'^([^_]+)')
  LEFT JOIN `of-scheduler-proj.core.v_caption_sentiment_v1` s USING (caption_hash)
  CROSS JOIN (
    SELECT * FROM `of-scheduler-proj.core.v_performance_alerts_v1` 
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  ) pa
)

-- Main validation report
SELECT 
  '=== EROS ENHANCED SYSTEM VALIDATION REPORT ===' AS report_header,
  CURRENT_TIMESTAMP() AS generated_at,
  
  -- Coverage summary (sorted by lowest completeness)
  ARRAY(
    SELECT AS STRUCT 
      dimension,
      total_records,
      classified_records,
      ROUND(coverage_pct * 100, 1) AS coverage_percentage,
      distinct_values
    FROM coverage_analysis 
    ORDER BY coverage_pct ASC
  ) AS coverage_by_dimension,
  
  -- Quality issues
  (SELECT quality_checks FROM quality_analysis) AS data_quality_issues,
  
  -- Performance metrics  
  (SELECT metrics FROM performance_metrics) AS system_performance,
  
  -- Feature utilization
  (SELECT utilization_stats FROM feature_utilization) AS feature_usage,
  
  -- Overall health score calculation
  ROUND(
    (SELECT AVG(coverage_pct) FROM coverage_analysis) * 100, 1
  ) AS overall_coverage_score,
  
  -- Recommendations
  ARRAY[
    CASE 
      WHEN (SELECT MIN(coverage_pct) FROM coverage_analysis) < 0.8 
      THEN 'URGENT: Address low coverage areas below 80%'
      ELSE 'Coverage levels acceptable'
    END,
    CASE
      WHEN (SELECT SUM(issue_count) FROM (SELECT check_name, issue_count FROM quality_analysis CROSS JOIN UNNEST(quality_checks))) > 100
      THEN 'WARNING: Multiple data quality issues detected'
      ELSE 'Data quality within acceptable ranges'  
    END,
    'Monitor system performance metrics regularly',
    'Ensure all new pages get content profiles',
    'Review and update cooldown settings based on performance'
  ] AS recommendations

UNION ALL

-- Detailed breakdown for each dimension
SELECT 
  '=== DETAILED COVERAGE BREAKDOWN ===' AS report_header,
  CURRENT_TIMESTAMP() AS generated_at,
  
  -- Individual dimension details
  coverage_by_dimension,
  data_quality_issues,
  system_performance,
  feature_usage,
  overall_coverage_score,
  recommendations
  
FROM (
  SELECT 
    coverage_by_dimension,
    data_quality_issues,
    system_performance, 
    feature_usage,
    overall_coverage_score,
    recommendations
  FROM (
    SELECT 
      ARRAY(SELECT AS STRUCT * FROM coverage_analysis ORDER BY coverage_pct ASC) AS coverage_by_dimension,
      (SELECT quality_checks FROM quality_analysis) AS data_quality_issues,
      (SELECT metrics FROM performance_metrics) AS system_performance,
      (SELECT utilization_stats FROM feature_utilization) AS feature_usage,
      ROUND((SELECT AVG(coverage_pct) FROM coverage_analysis) * 100, 1) AS overall_coverage_score,
      ARRAY['Validation complete - check individual metrics above'] AS recommendations
  )
)

ORDER BY report_header;

-- Additional detailed queries for troubleshooting

-- Show pages with missing content profiles
SELECT 
  'Pages Missing Content Profiles' AS analysis_type,
  pd.username_std,
  pd.is_active,
  pd.page_type,
  CASE WHEN p.username_std IS NULL THEN 'Missing Profile' ELSE 'Has Profile' END AS profile_status
FROM `of-scheduler-proj.core.page_dim` pd
LEFT JOIN `of-scheduler-proj.core.page_content_profile_v1` p USING (username_std)
WHERE pd.is_active = true
  AND p.username_std IS NULL
ORDER BY pd.username_std
LIMIT 20;

-- Show captions with data quality issues
SELECT 
  'Caption Data Quality Issues' AS analysis_type,
  caption_hash,
  CASE 
    WHEN caption_text IS NULL OR TRIM(caption_text) = '' THEN 'Missing Text'
    WHEN LENGTH(caption_text) > 2000 THEN 'Text Too Long'
    WHEN explicitness NOT IN ('explicit', 'moderate', 'mild', 'gfe-implied', 'other') THEN 'Invalid Explicitness'
    ELSE 'Unknown Issue'
  END AS issue_type,
  explicitness,
  LENGTH(caption_text) AS text_length
FROM `of-scheduler-proj.raw.caption_library`
WHERE caption_text IS NULL 
   OR TRIM(caption_text) = ''
   OR LENGTH(caption_text) > 2000
   OR (explicitness IS NOT NULL AND explicitness NOT IN ('explicit', 'moderate', 'mild', 'gfe-implied', 'other'))
ORDER BY issue_type, caption_hash
LIMIT 50;

-- Performance test summary
SELECT 
  'Performance Test Summary' AS test_type,
  'Smart Cooldowns Query' AS test_name,
  COUNT(*) AS records_processed,
  CURRENT_TIMESTAMP() AS test_timestamp
FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`

UNION ALL

SELECT 
  'Performance Test Summary' AS test_type,
  'Sentiment Analysis Query' AS test_name,
  COUNT(*) AS records_processed,
  CURRENT_TIMESTAMP() AS test_timestamp  
FROM `of-scheduler-proj.core.v_caption_sentiment_v1`

UNION ALL

SELECT 
  'Performance Test Summary' AS test_type,
  'Performance Alerts Query' AS test_name,
  COUNT(*) AS records_processed,
  CURRENT_TIMESTAMP() AS test_timestamp
FROM `of-scheduler-proj.core.v_performance_alerts_v1`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

ORDER BY test_name;