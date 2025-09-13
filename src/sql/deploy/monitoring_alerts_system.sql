-- =====================================================
-- EROS ENHANCED CAPTION SYSTEM - MONITORING & ALERTS
-- Phase D: Proactive Performance Monitoring & Interventions
-- Project: of-scheduler-proj
-- =====================================================

-- Configuration
DECLARE BASELINE_DAYS STRUCT<history INT64, holdout_last INT64> DEFAULT (30, 7);
DECLARE ALERT_LOOKBACK_DAYS INT64 DEFAULT 7;

-- =====================================================
-- PERFORMANCE ALERTS VIEW
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_performance_alerts_v1` AS
WITH baseline AS (
  SELECT 
    username_std,
    -- Revenue metrics
    AVG(earnings_usd) AS avg_daily_revenue,
    STDDEV(earnings_usd) AS revenue_stddev,
    APPROX_QUANTILES(earnings_usd, 100)[OFFSET(25)] AS revenue_p25,
    APPROX_QUANTILES(earnings_usd, 100)[OFFSET(75)] AS revenue_p75,
    APPROX_QUANTILES(earnings_usd, 100)[OFFSET(90)] AS revenue_p90,
    
    -- Conversion metrics
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS avg_conversion_rate,
    STDDEV(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS conversion_stddev,
    APPROX_QUANTILES(SAFE_DIVIDE(purchased, NULLIF(sent, 0)), 100)[OFFSET(25)] AS conversion_p25,
    
    -- Volume metrics
    AVG(sent) AS avg_daily_sent,
    STDDEV(sent) AS sent_stddev,
    
    -- Caption diversity
    AVG(caption_diversity) AS avg_caption_diversity,
    
    COUNT(*) AS baseline_days
    
  FROM (
    SELECT 
      username_std, 
      DATE(sending_ts) AS d, 
      SUM(earnings_usd) AS earnings_usd,
      SUM(sent) AS sent,
      SUM(purchased) AS purchased,
      COUNT(DISTINCT caption_hash) AS caption_diversity
    FROM `of-scheduler-proj.core.message_facts`
    WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)  -- BASELINE_DAYS.history
      AND sent < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)    -- BASELINE_DAYS.holdout_last
      AND username_std IS NOT NULL
    GROUP BY 1, 2
  )
  GROUP BY 1
  HAVING baseline_days >= 10  -- Minimum baseline data
),
current_performance AS (
  SELECT 
    username_std, 
    DATE(sending_ts) AS date, 
    SUM(earnings_usd) AS daily_revenue,
    SUM(sent) AS daily_sent,
    SUM(purchased) AS daily_purchased,
    COUNT(DISTINCT caption_hash) AS captions_used,
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS conversion_rate,
    MAX(sending_ts) AS latest_activity
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)  -- ALERT_LOOKBACK_DAYS
    AND username_std IS NOT NULL
  GROUP BY 1, 2
),
alert_analysis AS (
  SELECT
    c.username_std, 
    c.date, 
    c.daily_revenue,
    c.daily_sent,
    c.daily_purchased,
    c.captions_used,
    c.conversion_rate,
    c.latest_activity,
    
    -- Baseline comparisons
    b.avg_daily_revenue AS expected_revenue,
    b.revenue_stddev,
    b.revenue_p25,
    b.revenue_p75,
    b.revenue_p90,
    b.avg_conversion_rate AS expected_conversion,
    b.conversion_stddev,
    b.conversion_p25,
    b.avg_daily_sent AS expected_sent,
    b.avg_caption_diversity AS expected_caption_diversity,
    
    -- Z-scores
    SAFE_DIVIDE(c.daily_revenue - b.avg_daily_revenue, NULLIF(b.revenue_stddev, 0)) AS revenue_z_score,
    SAFE_DIVIDE(c.conversion_rate - b.avg_conversion_rate, NULLIF(b.conversion_stddev, 0)) AS conversion_z_score,
    SAFE_DIVIDE(c.daily_sent - b.avg_daily_sent, NULLIF(b.sent_stddev, 0)) AS volume_z_score,
    
    -- Performance ratios
    SAFE_DIVIDE(c.daily_revenue, NULLIF(b.avg_daily_revenue, 0)) AS revenue_ratio,
    SAFE_DIVIDE(c.conversion_rate, NULLIF(b.avg_conversion_rate, 0)) AS conversion_ratio,
    SAFE_DIVIDE(c.captions_used, NULLIF(b.avg_caption_diversity, 0)) AS diversity_ratio
    
  FROM current_performance c 
  JOIN baseline b USING (username_std)
)
SELECT
  username_std,
  date,
  daily_revenue,
  expected_revenue,
  revenue_z_score,
  conversion_rate,
  expected_conversion,
  conversion_z_score,
  captions_used,
  expected_caption_diversity,
  latest_activity,
  
  -- ALERT STATUS DETERMINATION
  CASE
    WHEN daily_revenue < revenue_p25 AND revenue_z_score < -2.0 THEN 'CRITICAL_LOW'
    WHEN daily_revenue < expected_revenue - revenue_stddev THEN 'WARNING'
    WHEN daily_revenue > revenue_p90 THEN 'EXCEEDING'
    WHEN daily_revenue > revenue_p75 THEN 'GOOD'
    ELSE 'NORMAL'
  END AS performance_status,
  
  -- SPECIFIC ISSUE FLAGS
  STRUCT(
    conversion_rate < conversion_p25 AS low_conversion,
    captions_used < expected_caption_diversity * 0.7 AS low_diversity,
    daily_sent < expected_sent * 0.8 AS low_volume,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), latest_activity, HOUR) > 8 AS stale_activity,
    revenue_z_score < -1.5 AS revenue_concern,
    conversion_z_score < -1.5 AS conversion_concern
  ) AS issue_flags,
  
  -- RECOMMENDED ACTIONS
  CASE
    WHEN daily_revenue < revenue_p25 THEN 
      'URGENT: Deploy high-performance captions from historical winners. Increase variety and check timing windows.'
    WHEN conversion_rate < conversion_p25 THEN 
      'Low conversion: Use high-intimacy/urgency captions, test lower price points, improve timing.'
    WHEN captions_used < expected_caption_diversity * 0.7 THEN 
      'Low diversity: Rotate more captions to prevent audience fatigue. Check cooldown overrides.'
    WHEN daily_sent < expected_sent * 0.8 THEN
      'Low volume: Check scheduler activity and auto-send settings. Review engagement patterns.'
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), latest_activity, HOUR) > 8 THEN
      'Stale activity: Check scheduler status and system health. May need manual intervention.'
    ELSE 'Performance within normal range. Monitor for trends.'
  END AS recommended_action,
  
  -- SEVERITY SCORE (0-100, higher = more urgent)
  LEAST(100, GREATEST(0, 
    50 - CAST(revenue_z_score * 15 AS INT64) +  -- Revenue impact
    CASE WHEN conversion_rate < conversion_p25 THEN 20 ELSE 0 END +  -- Conversion penalty
    CASE WHEN captions_used < expected_caption_diversity * 0.7 THEN 15 ELSE 0 END +  -- Diversity penalty
    CASE WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), latest_activity, HOUR) > 8 THEN 25 ELSE 0 END  -- Stale activity penalty
  )) AS severity_score,
  
  CURRENT_TIMESTAMP() AS alert_generated_at
  
FROM alert_analysis;

-- =====================================================
-- PERFORMANCE INTERVENTION STORED PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE `of-scheduler-proj.core.sp_performance_intervention_v1`(target_page STRING)
BEGIN
  DECLARE intervention_type STRING;
  DECLARE perf_status STRING;
  DECLARE severity INT64;
  
  -- Get current performance status
  SET (perf_status, severity) = (
    SELECT AS STRUCT performance_status, severity_score 
    FROM `of-scheduler-proj.core.v_performance_alerts_v1`
    WHERE username_std = target_page 
      AND date = CURRENT_DATE() 
    ORDER BY alert_generated_at DESC 
    LIMIT 1
  );
  
  -- Only intervene for concerning performance
  IF perf_status IN ('CRITICAL_LOW', 'WARNING') OR severity > 60 THEN
    
    -- Create intervention recommendations
    CREATE TEMP TABLE intervention_recommendations AS
    WITH recent_usage AS (
      SELECT 
        caption_hash,
        AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS recent_conversion,
        COUNT(*) AS times_used_7d,
        MAX(last_used_ts) AS last_used
      FROM `of-scheduler-proj.core.message_facts`
      WHERE username_std = target_page 
        AND sent >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      GROUP BY 1
    ),
    historical_performance AS (
      SELECT 
        caption_hash,
        AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS historical_conversion,
        COUNT(*) AS times_used_90d,
        AVG(earnings_usd) AS avg_earnings,
        MAX(sending_ts) AS last_historical_use
      FROM `of-scheduler-proj.core.message_facts`
      WHERE username_std = target_page
        AND sent >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        AND sent < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      GROUP BY 1
      HAVING historical_conversion > 0.05  -- Only high-converting captions
        AND times_used_90d >= 2  -- Minimum usage for reliability
    ),
    recovery_candidates AS (
      SELECT 
        h.caption_hash,
        h.historical_conversion,
        h.avg_earnings,
        h.times_used_90d,
        COALESCE(r.recent_conversion, 0) AS recent_conversion,
        COALESCE(r.times_used_7d, 0) AS recent_uses,
        sc.available_after,
        sc.cooldown_override_suggested,
        sc.emergency_override_eligible,
        cs.engagement_prediction_score,
        
        -- Recovery score calculation
        (h.historical_conversion * 0.4 + 
         SAFE_DIVIDE(h.avg_earnings, 50.0) * 0.3 +  -- Normalize earnings
         LEAST(1.0, SAFE_DIVIDE(h.times_used_90d, 10.0)) * 0.2 +  -- Usage confidence
         COALESCE(cs.engagement_prediction_score, 0.5) * 0.1
        ) AS recovery_score
        
      FROM historical_performance h
      LEFT JOIN recent_usage r USING (caption_hash)
      LEFT JOIN `of-scheduler-proj.core.v_smart_cooldowns_v1` sc 
        ON sc.caption_hash = h.caption_hash AND sc.username_std = target_page
      LEFT JOIN `of-scheduler-proj.core.v_caption_sentiment_v1` cs USING (caption_hash)
      WHERE COALESCE(r.times_used_7d, 0) < 2  -- Low recent usage
      ORDER BY recovery_score DESC
      LIMIT 10
    )
    SELECT * FROM recovery_candidates;
    
    -- Determine intervention type
    SET intervention_type = CASE
      WHEN severity >= 80 THEN 'EMERGENCY_RECOVERY'
      WHEN perf_status = 'CRITICAL_LOW' THEN 'AGGRESSIVE_RECOVERY'
      ELSE 'PROACTIVE_ADJUSTMENT'
    END;
    
    -- Log the intervention
    CREATE TABLE IF NOT EXISTS `of-scheduler-proj.core.performance_interventions_log` (
      intervention_id STRING,
      intervention_ts TIMESTAMP,
      username_std STRING,
      performance_status STRING,
      severity_score INT64,
      intervention_type STRING,
      recommended_captions ARRAY<STRUCT<
        caption_hash STRING,
        historical_conversion FLOAT64,
        avg_earnings FLOAT64,
        recovery_score FLOAT64,
        available_after TIMESTAMP,
        override_suggested BOOL
      >>,
      strategy_note STRING,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    
    INSERT INTO `of-scheduler-proj.core.performance_interventions_log`
    SELECT 
      GENERATE_UUID() AS intervention_id,
      CURRENT_TIMESTAMP() AS intervention_ts,
      target_page AS username_std,
      perf_status AS performance_status,
      severity AS severity_score,
      intervention_type,
      ARRAY_AGG(STRUCT(
        caption_hash,
        historical_conversion,
        avg_earnings,
        recovery_score,
        available_after,
        cooldown_override_suggested AS override_suggested
      )) AS recommended_captions,
      CONCAT(
        intervention_type, ': Deploy top ', COUNT(*), ' historical winners. ',
        'Avg conversion: ', ROUND(AVG(historical_conversion) * 100, 1), '%. ',
        COUNTIF(cooldown_override_suggested), ' eligible for override.'
      ) AS strategy_note,
      CURRENT_TIMESTAMP() AS created_at
    FROM intervention_recommendations
    WHERE recovery_score > 0.3;  -- Quality threshold
    
  END IF;
END;

-- =====================================================
-- SYSTEM HEALTH MONITORING
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_system_health_v1` AS
WITH recent_activity AS (
  SELECT
    COUNT(DISTINCT username_std) AS active_pages_24h,
    COUNT(*) AS total_messages_24h,
    SUM(earnings_usd) AS total_earnings_24h,
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS avg_conversion_24h,
    COUNT(DISTINCT caption_hash) AS unique_captions_24h
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
),
system_metrics AS (
  SELECT
    -- Caption system health
    COUNT(*) AS total_captions_available,
    COUNTIF(available_after <= CURRENT_TIMESTAMP()) AS captions_ready_now,
    COUNTIF(fatigue_score >= 0.8) AS captions_burnt_out,
    COUNTIF(cooldown_override_suggested) AS captions_override_eligible,
    
    -- Performance distribution
    COUNTIF(performance_tier = 'ELITE') AS elite_captions,
    COUNTIF(performance_tier = 'HIGH_PERFORMER') AS high_performer_captions,
    
    AVG(final_cooldown_days) AS avg_cooldown_days
  FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
),
alert_summary AS (
  SELECT
    COUNT(*) AS total_alerts_today,
    COUNTIF(performance_status = 'CRITICAL_LOW') AS critical_alerts,
    COUNTIF(performance_status = 'WARNING') AS warning_alerts,
    COUNTIF(performance_status = 'EXCEEDING') AS exceeding_alerts,
    AVG(severity_score) AS avg_severity_score
  FROM `of-scheduler-proj.core.v_performance_alerts_v1`
  WHERE date = CURRENT_DATE()
)
SELECT
  -- Activity metrics
  ra.active_pages_24h,
  ra.total_messages_24h,
  ra.total_earnings_24h,
  ra.avg_conversion_24h,
  ra.unique_captions_24h,
  
  -- Caption system metrics
  sm.total_captions_available,
  sm.captions_ready_now,
  sm.captions_burnt_out,
  sm.captions_override_eligible,
  sm.elite_captions,
  sm.high_performer_captions,
  sm.avg_cooldown_days,
  
  -- Alert metrics
  als.total_alerts_today,
  als.critical_alerts,
  als.warning_alerts,
  als.exceeding_alerts,
  als.avg_severity_score,
  
  -- Health scores (0-100)
  CASE
    WHEN ra.active_pages_24h = 0 THEN 0
    WHEN ra.avg_conversion_24h < 0.02 THEN 30
    WHEN ra.avg_conversion_24h < 0.05 THEN 60
    WHEN ra.avg_conversion_24h < 0.08 THEN 80
    ELSE 95
  END AS conversion_health_score,
  
  CASE
    WHEN sm.captions_ready_now < 100 THEN 20
    WHEN sm.captions_ready_now < 500 THEN 60
    WHEN sm.captions_ready_now < 1000 THEN 80
    ELSE 95
  END AS content_availability_score,
  
  CASE
    WHEN als.critical_alerts > 5 THEN 10
    WHEN als.critical_alerts > 2 THEN 40
    WHEN als.warning_alerts > 10 THEN 60
    WHEN als.avg_severity_score > 60 THEN 70
    ELSE 90
  END AS alert_health_score,
  
  CURRENT_TIMESTAMP() AS calculated_at
  
FROM recent_activity ra
CROSS JOIN system_metrics sm
CROSS JOIN alert_summary als;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Test alert system
SELECT 
  'Alert System Test' AS test_name,
  COUNT(*) AS pages_monitored,
  COUNTIF(performance_status = 'CRITICAL_LOW') AS critical_pages,
  COUNTIF(performance_status = 'WARNING') AS warning_pages,
  AVG(severity_score) AS avg_severity
FROM `of-scheduler-proj.core.v_performance_alerts_v1`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- Test system health
SELECT *
FROM `of-scheduler-proj.core.v_system_health_v1`;