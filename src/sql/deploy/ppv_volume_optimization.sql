-- PPV Volume Optimization System
-- Determines optimal daily PPV count per page based on performance metrics

-- Create volume optimization view
CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_ppv_volume_recommendations_v1` AS
WITH page_metrics AS (
  -- Get recent performance metrics per page
  SELECT 
    username_page,
    
    -- Conversion metrics (last 28 days)
    AVG(SAFE_DIVIDE(purchased_sum, sent_sum)) as avg_conversion_rate,
    SUM(sent_sum) as total_sends_28d,
    SUM(purchased_sum) as total_purchases_28d,
    SUM(revenue_sum) as total_revenue_28d,
    
    -- Volume tolerance metrics
    COUNT(DISTINCT date_local) as active_days_28d,
    AVG(daily_send_count) as avg_daily_sends,
    STDDEV(daily_send_count) as send_volume_variance,
    
    -- Performance at different volumes
    AVG(CASE WHEN daily_send_count <= 3 THEN SAFE_DIVIDE(purchased_sum, sent_sum) END) as conversion_low_volume,
    AVG(CASE WHEN daily_send_count BETWEEN 4 AND 7 THEN SAFE_DIVIDE(purchased_sum, sent_sum) END) as conversion_med_volume,
    AVG(CASE WHEN daily_send_count >= 8 THEN SAFE_DIVIDE(purchased_sum, sent_sum) END) as conversion_high_volume,
    
    -- Revenue per send at different volumes
    AVG(CASE WHEN daily_send_count <= 3 THEN SAFE_DIVIDE(revenue_sum, sent_sum) END) as rps_low_volume,
    AVG(CASE WHEN daily_send_count BETWEEN 4 AND 7 THEN SAFE_DIVIDE(revenue_sum, sent_sum) END) as rps_med_volume,
    AVG(CASE WHEN daily_send_count >= 8 THEN SAFE_DIVIDE(revenue_sum, sent_sum) END) as rps_high_volume
    
  FROM (
    -- Daily aggregates from message facts
    SELECT 
      username_page,
      DATE(sending_dt_local) as date_local,
      COUNT(*) as daily_send_count,
      SUM(sent) as sent_sum,
      SUM(purchased) as purchased_sum,
      SUM(earnings) as revenue_sum
    FROM `of-scheduler-proj.mart.v_dm_send_facts_v3`
    WHERE DATE(sending_dt_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
      AND dm_type IN ('ppv', 'mass')  -- PPV messages only
      AND sent > 0
    GROUP BY username_page, DATE(sending_dt_local)
  )
  GROUP BY username_page
),

page_tier AS (
  -- Determine page tier based on performance
  SELECT 
    pm.*,
    pt.tier_override,
    
    -- Calculate performance score (0-100)
    LEAST(100, GREATEST(0, 
      (COALESCE(avg_conversion_rate, 0) * 500) +  -- Conversion rate weight: 50%
      (COALESCE(total_revenue_28d, 0) / 100) +     -- Revenue weight: scaled
      (CASE WHEN avg_daily_sends > 5 THEN 25 ELSE 0 END)  -- Volume tolerance bonus
    )) as performance_score,
    
    -- Classify tier based on metrics
    CASE 
      WHEN pt.tier_override IS NOT NULL THEN pt.tier_override
      WHEN avg_conversion_rate >= 0.15 AND total_revenue_28d >= 2000 THEN 'high_volume'
      WHEN avg_conversion_rate >= 0.10 AND total_revenue_28d >= 800 THEN 'standard'  
      WHEN avg_conversion_rate >= 0.05 AND total_revenue_28d >= 200 THEN 'conservative'
      ELSE 'minimal'
    END as calculated_tier
    
  FROM page_metrics pm
  LEFT JOIN `of-scheduler-proj.core.page_tier_override_v1` pt 
    ON pt.username_page = pm.username_page AND pt.is_active = true
),

volume_recommendations AS (
  SELECT 
    username_page,
    calculated_tier as tier,
    performance_score,
    avg_conversion_rate,
    total_revenue_28d,
    
    -- Volume recommendations based on tier and performance
    CASE calculated_tier
      WHEN 'high_volume' THEN 
        CASE 
          WHEN performance_score >= 80 THEN '10-12'
          WHEN performance_score >= 60 THEN '8-10' 
          ELSE '6-8'
        END
      WHEN 'standard' THEN
        CASE 
          WHEN performance_score >= 70 THEN '6-8'
          WHEN performance_score >= 50 THEN '4-6'
          ELSE '3-5'
        END
      WHEN 'conservative' THEN
        CASE 
          WHEN performance_score >= 60 THEN '4-5'
          WHEN performance_score >= 40 THEN '3-4'
          ELSE '2-3'
        END
      ELSE '2-3'  -- minimal tier
    END as recommended_daily_volume,
    
    -- Numeric ranges for system use
    CASE calculated_tier
      WHEN 'high_volume' THEN 
        CASE 
          WHEN performance_score >= 80 THEN STRUCT(10 as min_sends, 12 as max_sends, 11 as optimal_sends)
          WHEN performance_score >= 60 THEN STRUCT(8 as min_sends, 10 as max_sends, 9 as optimal_sends)
          ELSE STRUCT(6 as min_sends, 8 as max_sends, 7 as optimal_sends)
        END
      WHEN 'standard' THEN
        CASE 
          WHEN performance_score >= 70 THEN STRUCT(6 as min_sends, 8 as max_sends, 7 as optimal_sends)
          WHEN performance_score >= 50 THEN STRUCT(4 as min_sends, 6 as max_sends, 5 as optimal_sends)
          ELSE STRUCT(3 as min_sends, 5 as max_sends, 4 as optimal_sends)
        END
      WHEN 'conservative' THEN
        CASE 
          WHEN performance_score >= 60 THEN STRUCT(4 as min_sends, 5 as max_sends, 4 as optimal_sends)
          WHEN performance_score >= 40 THEN STRUCT(3 as min_sends, 4 as max_sends, 3 as optimal_sends)
          ELSE STRUCT(2 as min_sends, 3 as max_sends, 2 as optimal_sends)
        END
      ELSE STRUCT(2 as min_sends, 3 as max_sends, 2 as optimal_sends)  -- minimal
    END as volume_range,
    
    -- Performance insights
    CASE 
      WHEN rps_high_volume > rps_med_volume * 1.1 THEN 'VOLUME_BOOST'  -- Benefits from high volume
      WHEN rps_low_volume > rps_med_volume * 1.1 THEN 'VOLUME_SENSITIVE'  -- Prefers lower volume
      ELSE 'VOLUME_NEUTRAL'
    END as volume_sensitivity,
    
    -- Quality warnings
    CASE 
      WHEN avg_conversion_rate < 0.05 THEN 'LOW_CONVERSION'
      WHEN total_sends_28d < 30 THEN 'INSUFFICIENT_DATA'
      WHEN send_volume_variance > avg_daily_sends THEN 'INCONSISTENT_VOLUME'
      ELSE 'HEALTHY'
    END as quality_flag,
    
    -- Revenue opportunity estimation
    ROUND(
      (volume_range.optimal_sends * COALESCE(rps_med_volume, rps_low_volume, 5.0) * 30), 
      0
    ) as projected_monthly_revenue_usd,
    
    CURRENT_TIMESTAMP() as recommendation_ts
    
  FROM page_tier
)

SELECT * FROM volume_recommendations;

-- Create materialized table for fast access
CREATE OR REPLACE TABLE `of-scheduler-proj.mart.ppv_volume_recommendations_current` 
PARTITION BY DATE(recommendation_ts)
CLUSTER BY username_page
AS SELECT * FROM `of-scheduler-proj.mart.v_ppv_volume_recommendations_v1`;

-- Create view with intelligent volume scheduling
CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_ppv_daily_schedule_with_volume_v1` AS
WITH volume_recs AS (
  SELECT * FROM `of-scheduler-proj.mart.ppv_volume_recommendations_current`
  WHERE DATE(recommendation_ts) = CURRENT_DATE()
),

base_schedule AS (
  SELECT 
    t.*,
    v.tier,
    v.volume_range,
    v.recommended_daily_volume,
    v.volume_sensitivity,
    v.quality_flag,
    v.projected_monthly_revenue_usd
  FROM `of-scheduler-proj.mart.v_weekly_template_7d_pages_final` t
  LEFT JOIN volume_recs v ON v.username_page = t.username_page
  WHERE t.date_local = CURRENT_DATE()
),

-- Rank slots by performance score within each page
ranked_slots AS (
  SELECT 
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.username_page, s.date_local 
      ORDER BY sc.score_final DESC
    ) as slot_performance_rank
  FROM base_schedule s
  LEFT JOIN `of-scheduler-proj.mart.v_slot_scorecard_v3` sc 
    ON sc.username_page = s.username_page 
    AND sc.dow = MOD(EXTRACT(DAYOFWEEK FROM s.date_local) + 5, 7)
    AND sc.hod = s.hod_local
),

-- Select optimal number of slots per page based on volume recommendations
optimal_schedule AS (
  SELECT 
    rs.*,
    CASE 
      WHEN rs.slot_performance_rank <= COALESCE(rs.volume_range.optimal_sends, 4) THEN 'RECOMMENDED'
      WHEN rs.slot_performance_rank <= COALESCE(rs.volume_range.max_sends, 6) THEN 'OPTIONAL'
      ELSE 'EXCESS'
    END as slot_priority,
    
    -- Revenue impact estimation per slot
    ROUND(rs.price_usd * COALESCE(sc.conversion_rate_smoothed, 0.08), 2) as projected_slot_revenue
    
  FROM ranked_slots rs
  LEFT JOIN `of-scheduler-proj.mart.v_slot_scorecard_v3` sc 
    ON sc.username_page = rs.username_page 
    AND sc.dow = MOD(EXTRACT(DAYOFWEEK FROM rs.date_local) + 5, 7)
    AND sc.hod = rs.hod_local
)

SELECT 
  *,
  -- Smart scheduling guidance
  CASE 
    WHEN quality_flag = 'LOW_CONVERSION' THEN 'Focus on caption quality and timing optimization'
    WHEN quality_flag = 'INSUFFICIENT_DATA' THEN 'Start with conservative volume to gather data'
    WHEN volume_sensitivity = 'VOLUME_SENSITIVE' THEN 'Reduce volume - quality over quantity approach'
    WHEN volume_sensitivity = 'VOLUME_BOOST' THEN 'Increase volume - page handles high frequency well'
    WHEN slot_priority = 'EXCESS' THEN 'Consider skipping this slot - diminishing returns'
    ELSE 'Optimal slot - high conversion probability'
  END as scheduling_guidance

FROM optimal_schedule
ORDER BY username_page, slot_performance_rank;