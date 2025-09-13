-- =====================================================
-- VOLUME OPTIMIZATION FOR ML 7-DAY SCHEDULE BUILDER
-- =====================================================
-- This file contains views for intelligent PPV volume recommendations
-- based on page performance, conversion rates, and engagement patterns.

-- Volume Optimization Logic: v_ppv_volume_recommendations
-- Determines optimal daily send volume (2-12) based on page performance tier,
-- conversion rates, audience size, and historical patterns.
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_ppv_volume_recommendations` AS
WITH 
-- Base page performance metrics (last 90 days)
page_performance AS (
  SELECT
    username_page,
    -- Core metrics for volume decisions
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS avg_conversion_rate,
    AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS avg_rps,
    AVG(SAFE_DIVIDE(viewed, NULLIF(sent, 0))) AS avg_view_rate,
    COUNT(*) AS sends_90d,
    SUM(earnings_usd) AS total_revenue_90d,
    SUM(sent) AS total_sent_90d,
    SUM(purchased) AS total_purchased_90d,
    
    -- Volatility metrics (consistency indicator)
    STDDEV(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS conversion_volatility,
    STDDEV(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS rps_volatility,
    
    -- Recent trend (last 30 vs 30-90 days ago)
    AVG(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
             THEN SAFE_DIVIDE(purchased, NULLIF(sent, 0)) END) AS conversion_rate_30d,
    AVG(CASE WHEN sending_ts < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
             THEN SAFE_DIVIDE(purchased, NULLIF(sent, 0)) END) AS conversion_rate_30_90d
  FROM `of-scheduler-proj.core.v_message_facts_by_page_90d`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND sent > 0
  GROUP BY username_page
),

-- Page demographics and tier information
page_context AS (
  SELECT
    v.username_page,
    v.username_std,
    v.page_type,
    
    -- Get tier info
    COALESCE(t.tier_final, 'standard') AS tier,
    
    -- Page state (grow/retain/monetize/balance)
    COALESCE(ps.page_state, 'balance') AS page_state,
    
    -- Subscriber metrics
    COALESCE(cs.active_fans, 0) AS active_fans,
    COALESCE(cs.new_fans, 0) AS new_fans,
    COALESCE(cs.renew_on_pct, 0) AS renew_rate,
    
    -- Revenue context
    COALESCE(ps.rev_7d, 0) AS rev_7d,
    COALESCE(ps.rev_28d, 0) AS rev_28d
    
  FROM `of-scheduler-proj.core.v_pages` v
  LEFT JOIN `of-scheduler-proj.core.v_page_tier_final_v1` t USING (username_std)  
  LEFT JOIN `of-scheduler-proj.core.page_state` ps USING (username_std)
  LEFT JOIN `of-scheduler-proj.staging.creator_stats_latest` cs USING (username_std)
),

-- Engagement patterns from new v_page_engagement_patterns_v1
engagement_patterns AS (
  SELECT
    CONCAT(username_std, '__main') AS username_page, -- Default to main page type
    volume_tier,
    avg_conversion_rate AS pattern_conversion_rate,
    total_90d_messages,
    conversion_volatility AS pattern_volatility,
    schedule_coverage
  FROM `of-scheduler-proj.core.v_page_engagement_patterns_v1`
),

-- Current quota from existing system
current_quota AS (
  SELECT
    username_std,
    dow,
    ppv_quota AS current_quota,
    is_burst_dow
  FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`
),

-- Aggregate current quota for general assessment
avg_current_quota AS (
  SELECT
    username_std,
    AVG(ppv_quota) AS avg_current_quota,
    MAX(ppv_quota) AS max_current_quota,
    COUNT(DISTINCT CASE WHEN is_burst_dow THEN dow END) AS burst_days_count
  FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`
  GROUP BY username_std
),

-- Calculate volume recommendations
volume_calculation AS (
  SELECT
    pc.username_page,
    pc.username_std,
    pc.tier,
    pc.page_state,
    pc.page_type,
    pc.active_fans,
    pc.new_fans,
    pc.renew_rate,
    
    -- Performance metrics
    COALESCE(pp.avg_conversion_rate, 0) AS avg_conversion_rate,
    COALESCE(pp.avg_rps, 0) AS avg_rps,
    COALESCE(pp.avg_view_rate, 0) AS avg_view_rate,
    COALESCE(pp.sends_90d, 0) AS sends_90d,
    COALESCE(pp.total_revenue_90d, 0) AS total_revenue_90d,
    
    -- Stability metrics
    COALESCE(pp.conversion_volatility, 1.0) AS conversion_volatility,
    COALESCE(pp.rps_volatility, 1.0) AS rps_volatility,
    
    -- Trend analysis
    COALESCE(pp.conversion_rate_30d, 0) AS conversion_rate_30d,
    COALESCE(pp.conversion_rate_30_90d, 0) AS conversion_rate_30_90d,
    SAFE_DIVIDE(COALESCE(pp.conversion_rate_30d, 0), NULLIF(COALESCE(pp.conversion_rate_30_90d, 0), 0)) AS conversion_trend_ratio,
    
    -- Engagement context
    COALESCE(ep.volume_tier, 'MINIMAL_VOLUME') AS volume_tier,
    COALESCE(ep.pattern_conversion_rate, 0) AS pattern_conversion_rate,
    COALESCE(ep.schedule_coverage, 0) AS schedule_coverage,
    
    -- Current quota context  
    COALESCE(acq.avg_current_quota, 4) AS avg_current_quota,
    COALESCE(acq.max_current_quota, 4) AS max_current_quota,
    COALESCE(acq.burst_days_count, 0) AS burst_days_count
    
  FROM page_context pc
  LEFT JOIN page_performance pp USING (username_page)
  LEFT JOIN engagement_patterns ep USING (username_page)  
  LEFT JOIN avg_current_quota acq USING (username_std)
),

-- Apply volume recommendation logic
volume_recommendations AS (
  SELECT
    vc.*,
    
    -- Base volume calculation using multiple signals
    CASE
      -- High performers: 8-12 sends
      WHEN vc.tier IN ('top_tier', 'high_performance') 
           AND vc.avg_conversion_rate > 0.15 
           AND vc.active_fans > 500
           AND vc.volume_tier = 'HIGH_VOLUME'
      THEN 10
      
      WHEN vc.tier IN ('top_tier', 'high_performance') 
           AND vc.avg_conversion_rate > 0.12 
           AND vc.active_fans > 200
      THEN 8
      
      -- Growth pages: moderate volume with conversion focus
      WHEN vc.page_state = 'grow' 
           AND vc.new_fans > 20 
           AND vc.avg_conversion_rate > 0.08
      THEN 6
      
      -- Standard performers: 4-6 sends
      WHEN vc.tier = 'standard' 
           AND vc.avg_conversion_rate > 0.10 
           AND vc.volume_tier IN ('MEDIUM_VOLUME', 'HIGH_VOLUME')
      THEN 5
      
      WHEN vc.tier = 'standard' 
           AND vc.avg_conversion_rate > 0.08
      THEN 4
      
      -- Retention focus: conservative volume
      WHEN vc.page_state = 'retain' 
           AND vc.renew_rate < 0.25
      THEN 3
      
      -- New or underperforming: start low
      WHEN vc.sends_90d < 50 
           OR vc.avg_conversion_rate < 0.05
      THEN 2
      
      -- Default based on tier
      WHEN vc.tier IN ('top_tier', 'high_performance') THEN 6
      WHEN vc.tier = 'standard' THEN 4
      ELSE 3
    END AS base_recommended_volume,
    
    -- Adjustment factors
    CASE
      WHEN vc.conversion_trend_ratio > 1.2 THEN 1.2  -- Improving trend
      WHEN vc.conversion_trend_ratio > 1.1 THEN 1.1
      WHEN vc.conversion_trend_ratio < 0.8 THEN 0.8  -- Declining trend  
      WHEN vc.conversion_trend_ratio < 0.9 THEN 0.9
      ELSE 1.0
    END AS trend_multiplier,
    
    CASE
      WHEN vc.conversion_volatility < 0.05 THEN 1.1   -- Very stable
      WHEN vc.conversion_volatility < 0.10 THEN 1.05  -- Stable
      WHEN vc.conversion_volatility > 0.30 THEN 0.9   -- Volatile
      WHEN vc.conversion_volatility > 0.20 THEN 0.95  -- Somewhat volatile
      ELSE 1.0
    END AS stability_multiplier,
    
    CASE
      WHEN vc.schedule_coverage > 0.5 THEN 1.1   -- Good schedule diversity
      WHEN vc.schedule_coverage > 0.3 THEN 1.05
      WHEN vc.schedule_coverage < 0.1 THEN 0.9   -- Limited schedule coverage
      ELSE 1.0
    END AS coverage_multiplier
    
  FROM volume_calculation vc
),

-- Final volume recommendations with bounds
final_recommendations AS (
  SELECT
    vr.*,
    
    -- Apply all multipliers
    vr.base_recommended_volume * 
    vr.trend_multiplier * 
    vr.stability_multiplier * 
    vr.coverage_multiplier AS adjusted_volume,
    
    -- Clamp to 2-12 range with intelligent rounding
    GREATEST(2, LEAST(12, 
      CAST(ROUND(
        vr.base_recommended_volume * 
        vr.trend_multiplier * 
        vr.stability_multiplier * 
        vr.coverage_multiplier
      ) AS INT64)
    )) AS recommended_daily_sends,
    
    -- Calculate volume change vs current
    GREATEST(2, LEAST(12, 
      CAST(ROUND(
        vr.base_recommended_volume * 
        vr.trend_multiplier * 
        vr.stability_multiplier * 
        vr.coverage_multiplier
      ) AS INT64)
    )) - CAST(vr.avg_current_quota AS INT64) AS volume_change_vs_current,
    
    CURRENT_TIMESTAMP() AS calculated_at
    
  FROM volume_recommendations vr
)

SELECT
  fr.username_page,
  fr.username_std, 
  fr.tier,
  fr.page_state,
  fr.page_type,
  
  -- Core metrics that drove the decision
  fr.avg_conversion_rate,
  fr.avg_rps,
  fr.active_fans,
  fr.volume_tier,
  fr.sends_90d,
  fr.total_revenue_90d,
  
  -- Stability and trend indicators
  fr.conversion_volatility,
  fr.conversion_trend_ratio,
  fr.schedule_coverage,
  
  -- Current vs recommended
  CAST(fr.avg_current_quota AS INT64) AS current_avg_quota,
  fr.recommended_daily_sends,
  fr.volume_change_vs_current,
  
  -- Explanation of factors
  fr.base_recommended_volume,
  fr.trend_multiplier,
  fr.stability_multiplier, 
  fr.coverage_multiplier,
  fr.adjusted_volume,
  
  -- Reasoning
  CASE
    WHEN fr.recommended_daily_sends >= 8 THEN 'High-volume: Strong conversion rate and audience size support frequent messaging'
    WHEN fr.recommended_daily_sends >= 6 THEN 'Medium-high: Good performance metrics suggest above-average volume'
    WHEN fr.recommended_daily_sends >= 4 THEN 'Standard: Balanced approach based on consistent performance'
    WHEN fr.recommended_daily_sends = 3 THEN 'Conservative: Focus on retention or building consistency'
    ELSE 'Minimal: Start low while establishing performance patterns'
  END AS volume_reasoning,
  
  -- Confidence scoring
  CASE
    WHEN fr.sends_90d >= 100 AND fr.conversion_volatility < 0.15 THEN 'HIGH'
    WHEN fr.sends_90d >= 50 AND fr.conversion_volatility < 0.25 THEN 'MEDIUM'
    WHEN fr.sends_90d >= 20 THEN 'LOW'
    ELSE 'VERY_LOW'
  END AS recommendation_confidence,
  
  fr.calculated_at

FROM final_recommendations fr
ORDER BY fr.username_page;


-- Volume Recommendations by Day of Week
-- Provides DOW-specific volume recommendations based on engagement patterns
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_ppv_volume_recommendations_dow` AS
WITH
-- DOW-specific performance from existing data
dow_performance AS (
  SELECT
    username_page,
    dow,
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS dow_conversion_rate,
    AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS dow_rps,
    COUNT(*) AS dow_sends,
    SUM(earnings_usd) AS dow_revenue,
    
    -- DOW performance vs page average
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) / 
    AVG(AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0)))) OVER (PARTITION BY username_page) AS dow_conversion_ratio
    
  FROM `of-scheduler-proj.core.v_message_facts_by_page_90d` m
  JOIN (
    SELECT DISTINCT username_page FROM `of-scheduler-proj.core.v_pages`
  ) p USING (username_page)
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND sent > 0
    AND EXTRACT(DAYOFWEEK FROM sending_ts) IS NOT NULL
  GROUP BY username_page, EXTRACT(DAYOFWEEK FROM sending_ts)
),

-- Base recommendations 
base_volume AS (
  SELECT
    username_page,
    username_std,
    recommended_daily_sends AS base_volume,
    recommendation_confidence,
    tier,
    page_state
  FROM `of-scheduler-proj.core.v_ppv_volume_recommendations`
),

-- Apply DOW adjustments
dow_adjusted AS (
  SELECT
    bv.username_page,
    bv.username_std,
    dp.dow,
    bv.base_volume,
    bv.recommendation_confidence,
    bv.tier,
    bv.page_state,
    
    COALESCE(dp.dow_conversion_rate, 0) AS dow_conversion_rate,
    COALESCE(dp.dow_conversion_ratio, 1.0) AS dow_performance_ratio,
    COALESCE(dp.dow_sends, 0) AS dow_historical_sends,
    
    -- Adjust volume based on DOW performance
    GREATEST(1, LEAST(15,
      CAST(ROUND(
        bv.base_volume * 
        CASE
          WHEN dp.dow_conversion_ratio > 1.3 THEN 1.4   -- Strong DOW
          WHEN dp.dow_conversion_ratio > 1.15 THEN 1.2  -- Good DOW
          WHEN dp.dow_conversion_ratio > 1.05 THEN 1.1  -- Slightly above avg
          WHEN dp.dow_conversion_ratio < 0.7 THEN 0.7   -- Weak DOW
          WHEN dp.dow_conversion_ratio < 0.85 THEN 0.85 -- Below average DOW
          ELSE 1.0
        END
      ) AS INT64)
    )) AS dow_recommended_volume
    
  FROM base_volume bv
  CROSS JOIN UNNEST([1,2,3,4,5,6,7]) AS dow  -- 1=Monday, 7=Sunday
  LEFT JOIN dow_performance dp USING (username_page, dow)
)

SELECT
  da.username_page,
  da.username_std,
  da.dow,
  
  -- DOW context
  CASE da.dow
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday' 
    WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'
    WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday'
    WHEN 7 THEN 'Sunday'
  END AS dow_name,
  
  -- Volume recommendations
  da.base_volume,
  da.dow_recommended_volume,
  da.dow_recommended_volume - da.base_volume AS dow_adjustment,
  
  -- Performance context
  da.dow_conversion_rate,
  da.dow_performance_ratio,
  da.dow_historical_sends,
  
  -- Metadata
  da.recommendation_confidence,
  da.tier,
  da.page_state,
  
  -- Reasoning
  CASE 
    WHEN da.dow_performance_ratio > 1.3 THEN 'Strong DOW: Increase volume significantly'
    WHEN da.dow_performance_ratio > 1.15 THEN 'Good DOW: Moderate volume increase'
    WHEN da.dow_performance_ratio > 1.05 THEN 'Above average DOW: Slight volume increase'
    WHEN da.dow_performance_ratio < 0.7 THEN 'Weak DOW: Reduce volume substantially'
    WHEN da.dow_performance_ratio < 0.85 THEN 'Below average DOW: Moderate volume reduction'
    WHEN da.dow_historical_sends < 5 THEN 'Limited data: Use base recommendation'
    ELSE 'Average DOW: Use base volume'
  END AS dow_reasoning,
  
  CURRENT_TIMESTAMP() AS calculated_at

FROM dow_adjusted da
ORDER BY da.username_page, da.dow;