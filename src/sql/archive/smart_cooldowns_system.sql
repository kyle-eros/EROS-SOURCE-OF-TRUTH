-- =====================================================
-- EROS ENHANCED CAPTION SYSTEM - SMART COOLDOWNS
-- Phase C: Multi-Factor Cooldown System with last_used_ts
-- Project: of-scheduler-proj
-- =====================================================

-- =====================================================
-- SMART COOLDOWNS VIEW (Exposes last_used_ts & available_after)
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_smart_cooldowns_v1` AS
WITH base AS (
  SELECT
    c.caption_id,
    r.username_page,
    c.explicitness,
    f.fatigue_score,
    COALESCE(r.rps_eb_price, 0) AS rps,
    r.score_final,
    r.updated_at,
    lu.last_used_ts,  -- CRITICAL: Exposed for Apps Script
    -- Count how many different pages use this caption
    COUNT(DISTINCT r.username_page) OVER (PARTITION BY c.caption_id) AS pages_using_caption,
    -- Get engagement prediction from sentiment analysis
    COALESCE(s.engagement_prediction_score, 0.5) AS engagement_score,
    -- Get page performance tier
    COALESCE(ep.volume_tier, 'MINIMAL_VOLUME') AS page_volume_tier
  FROM `of-scheduler-proj.core.caption_dim` c
  JOIN `of-scheduler-proj.mart.caption_rank_next24_v3_tbl` r USING (caption_id)
  LEFT JOIN `of-scheduler-proj.core.caption_fatigue_scores_v1` f 
    ON f.caption_id = c.caption_id AND f.username_page = r.username_page
  LEFT JOIN `of-scheduler-proj.core.v_caption_last_used_v3` lu 
    ON lu.caption_id = c.caption_id AND lu.username_page = r.username_page
  LEFT JOIN `of-scheduler-proj.core.v_caption_sentiment_v1` s USING (caption_id)
  LEFT JOIN `of-scheduler-proj.core.v_page_engagement_patterns_v1` ep 
    ON ep.username_page = r.username_page
),
performance_quartiles AS (
  SELECT 
    APPROX_QUANTILES(rps, 100)[OFFSET(75)] AS rps_p75,
    APPROX_QUANTILES(rps, 100)[OFFSET(90)] AS rps_p90,
    APPROX_QUANTILES(score_final, 100)[OFFSET(75)] AS score_p75
  FROM base
),
cooldown_calculation AS (
  SELECT
    b.*,
    pq.rps_p75,
    pq.rps_p90,
    pq.score_p75,
    -- Rolling 7-day performance score
    AVG(b.score_final) OVER (
      PARTITION BY b.caption_id 
      ORDER BY b.updated_at 
      ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_score,
    
    -- BASE COOLDOWN CALCULATION
    CASE
      -- Elite performers get shorter cooldowns
      WHEN b.rps > pq.rps_p90 THEN 2
      WHEN b.rps > pq.rps_p75 THEN 3
      -- Explicitness-based defaults
      WHEN b.explicitness = 'explicit' THEN 10
      WHEN b.explicitness = 'moderate' THEN 7
      WHEN b.explicitness IN ('mild', 'gfe-implied') THEN 5
      ELSE 5
    END AS base_cooldown_days,
    
    -- FATIGUE MULTIPLIER
    CASE
      WHEN COALESCE(b.fatigue_score, 0) >= 0.8 THEN 1.8  -- Burnt out: 80% longer cooldown
      WHEN COALESCE(b.fatigue_score, 0) >= 0.5 THEN 1.4  -- Fatigued: 40% longer
      WHEN COALESCE(b.fatigue_score, 0) >= 0.3 THEN 1.2  -- Moderate: 20% longer
      ELSE 1.0  -- Fresh: no penalty
    END AS fatigue_multiplier,
    
    -- CROSS-PAGE USAGE MULTIPLIER
    CASE
      WHEN b.pages_using_caption > 5 THEN 1.5  -- Heavily shared: 50% longer
      WHEN b.pages_using_caption > 3 THEN 1.3  -- Moderately shared: 30% longer
      WHEN b.pages_using_caption > 1 THEN 1.1  -- Lightly shared: 10% longer
      ELSE 1.0  -- Page-exclusive: no penalty
    END AS cross_page_multiplier,
    
    -- PAGE VOLUME ADJUSTMENT
    CASE
      WHEN b.page_volume_tier = 'HIGH_VOLUME' THEN 0.8    -- High volume pages get 20% shorter cooldowns
      WHEN b.page_volume_tier = 'MEDIUM_VOLUME' THEN 0.9  -- Medium volume: 10% shorter
      WHEN b.page_volume_tier = 'LOW_VOLUME' THEN 1.0     -- Low volume: no adjustment
      ELSE 1.2  -- Minimal volume: 20% longer to preserve content
    END AS volume_adjustment
    
  FROM base b
  CROSS JOIN performance_quartiles pq
)
SELECT
  caption_id, 
  username_page, 
  explicitness, 
  fatigue_score, 
  last_used_ts,  -- CRITICAL: Apps Script expects this field
  base_cooldown_days,
  pages_using_caption,
  rolling_7d_score,
  engagement_score,
  page_volume_tier,
  fatigue_multiplier,
  cross_page_multiplier,
  volume_adjustment,
  
  -- FINAL COOLDOWN CALCULATION
  GREATEST(
    CAST(
      base_cooldown_days * 
      fatigue_multiplier * 
      cross_page_multiplier * 
      volume_adjustment
    AS INT64), 
    1  -- Minimum 1 day cooldown
  ) AS final_cooldown_days,
  
  -- AVAILABLE AFTER TIMESTAMP (Critical for Apps Script)
  CASE
    WHEN last_used_ts IS NULL THEN CURRENT_TIMESTAMP()  -- Never used = available now
    ELSE TIMESTAMP_ADD(
      last_used_ts, 
      INTERVAL CAST(
        base_cooldown_days * 
        fatigue_multiplier * 
        cross_page_multiplier * 
        volume_adjustment
      AS INT64) DAY
    )
  END AS available_after,
  
  -- OVERRIDE RECOMMENDATIONS
  (rolling_7d_score > score_p75 AND 
   COALESCE(fatigue_score, 0) < 0.3 AND
   engagement_score > 0.6
  ) AS cooldown_override_suggested,
  
  -- EMERGENCY OVERRIDE (for critical content needs)
  (rolling_7d_score > score_p75 AND 
   rps > rps_p75 AND
   COALESCE(fatigue_score, 0) < 0.5
  ) AS emergency_override_eligible,
  
  -- PERFORMANCE TIER
  CASE
    WHEN rps > rps_p90 THEN 'ELITE'
    WHEN rps > rps_p75 THEN 'HIGH_PERFORMER'
    WHEN rps > 0 THEN 'AVERAGE'
    ELSE 'UNTESTED'
  END AS performance_tier,
  
  CURRENT_TIMESTAMP() AS calculated_at
  
FROM cooldown_calculation;

-- =====================================================
-- COOLDOWN SETTINGS TABLE (for runtime configuration)
-- =====================================================

CREATE TABLE IF NOT EXISTS `of-scheduler-proj.core.cooldown_settings_v1` (
  setting_key STRING,
  setting_value STRING,
  description STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_by STRING
);

-- Insert default settings
INSERT INTO `of-scheduler-proj.core.cooldown_settings_v1` 
VALUES 
  ('enable_smart_cooldowns', 'true', 'Master switch for smart cooldown system', CURRENT_TIMESTAMP(), 'system'),
  ('emergency_override_enabled', 'true', 'Allow emergency cooldown overrides for elite content', CURRENT_TIMESTAMP(), 'system'),
  ('min_cooldown_hours', '6', 'Absolute minimum cooldown in hours', CURRENT_TIMESTAMP(), 'system'),
  ('max_cooldown_days', '21', 'Maximum cooldown in days regardless of calculations', CURRENT_TIMESTAMP(), 'system'),
  ('fatigue_threshold_warning', '0.5', 'Fatigue score that triggers warnings', CURRENT_TIMESTAMP(), 'system'),
  ('fatigue_threshold_block', '0.8', 'Fatigue score that blocks usage', CURRENT_TIMESTAMP(), 'system')
ON CONFLICT (setting_key) DO UPDATE SET
  setting_value = EXCLUDED.setting_value,
  updated_at = CURRENT_TIMESTAMP();

-- =====================================================
-- COOLDOWN HELPER FUNCTIONS
-- =====================================================

-- Function to check if caption is available for specific page
CREATE OR REPLACE FUNCTION `of-scheduler-proj.core.is_caption_available`(
  caption_id_param STRING, 
  username_page_param STRING
) 
RETURNS BOOLEAN 
AS (
  EXISTS(
    SELECT 1 
    FROM `of-scheduler-proj.core.v_smart_cooldowns_v1` 
    WHERE caption_id = caption_id_param 
      AND username_page = username_page_param
      AND available_after <= CURRENT_TIMESTAMP()
  )
);

-- Function to get next available time for caption
CREATE OR REPLACE FUNCTION `of-scheduler-proj.core.get_caption_available_time`(
  caption_id_param STRING, 
  username_page_param STRING
) 
RETURNS TIMESTAMP 
AS (
  (SELECT available_after 
   FROM `of-scheduler-proj.core.v_smart_cooldowns_v1` 
   WHERE caption_id = caption_id_param 
     AND username_page = username_page_param
   LIMIT 1)
);

-- =====================================================
-- MATERIALIZED VIEW FOR PERFORMANCE
-- =====================================================

-- Create materialized view for faster Apps Script queries
CREATE MATERIALIZED VIEW IF NOT EXISTS `of-scheduler-proj.core.mv_caption_cooldowns_fast`
PARTITION BY DATE(calculated_at)
CLUSTER BY username_page, performance_tier
AS
SELECT 
  caption_id,
  username_page,
  final_cooldown_days,
  available_after,
  cooldown_override_suggested,
  emergency_override_eligible,
  performance_tier,
  fatigue_score,
  last_used_ts,
  calculated_at
FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`;

-- =====================================================
-- VERIFICATION & TESTING QUERIES
-- =====================================================

-- Test cooldown distribution
SELECT 
  'Cooldown Distribution' AS test_name,
  performance_tier,
  COUNT(*) AS caption_count,
  AVG(final_cooldown_days) AS avg_cooldown_days,
  MIN(final_cooldown_days) AS min_cooldown,
  MAX(final_cooldown_days) AS max_cooldown,
  COUNT(*) FILTER(WHERE cooldown_override_suggested) AS override_suggestions,
  COUNT(*) FILTER(WHERE emergency_override_eligible) AS emergency_eligible
FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
GROUP BY performance_tier
ORDER BY avg_cooldown_days;

-- Test availability counts
SELECT 
  'Availability Test' AS test_name,
  COUNT(*) AS total_caption_page_pairs,
  COUNT(*) FILTER(WHERE available_after <= CURRENT_TIMESTAMP()) AS currently_available,
  COUNT(*) FILTER(WHERE available_after > CURRENT_TIMESTAMP()) AS in_cooldown,
  COUNT(*) FILTER(WHERE last_used_ts IS NULL) AS never_used
FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`;

-- Test fatigue impact
SELECT 
  'Fatigue Impact Test' AS test_name,
  CASE
    WHEN fatigue_score >= 0.8 THEN 'BURNT_OUT'
    WHEN fatigue_score >= 0.5 THEN 'FATIGUED'
    WHEN fatigue_score >= 0.3 THEN 'MODERATE'
    ELSE 'FRESH'
  END AS fatigue_tier,
  COUNT(*) AS count,
  AVG(final_cooldown_days) AS avg_cooldown,
  AVG(base_cooldown_days) AS avg_base_cooldown,
  AVG(fatigue_multiplier) AS avg_fatigue_multiplier
FROM `of-scheduler-proj.core.v_smart_cooldowns_v1`
WHERE fatigue_score IS NOT NULL
GROUP BY fatigue_tier
ORDER BY avg_cooldown;