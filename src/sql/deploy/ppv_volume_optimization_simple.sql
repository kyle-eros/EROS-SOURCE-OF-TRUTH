-- PPV Volume Optimization System - Simplified Version
-- Step 1: Create core volume recommendations

-- First create the page tier override table if it doesn't exist
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.core.page_tier_override_v1` (
  username_page STRING NOT NULL,
  tier_override STRING,
  is_active BOOL DEFAULT TRUE,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  notes STRING
);

-- Create volume optimization view  
CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_ppv_volume_recommendations_v1` AS
WITH recent_performance AS (
  -- Get performance metrics from the last 28 days
  SELECT 
    username_page,
    COUNT(DISTINCT DATE(sending_dt_local)) as active_days_28d,
    COUNT(*) as total_sends_28d,
    SUM(sent) as total_sent_count,
    SUM(purchased) as total_purchased_count, 
    SUM(earnings) as total_revenue_28d,
    AVG(SAFE_DIVIDE(purchased, sent)) as avg_conversion_rate,
    AVG(SAFE_DIVIDE(earnings, sent)) as avg_revenue_per_send,
    
    -- Daily send count analysis
    AVG(daily_send_count) as avg_daily_sends,
    STDDEV(daily_send_count) as send_variance
  FROM (
    SELECT 
      username_page,
      DATE(sending_dt_local) as send_date,
      COUNT(*) as daily_send_count,
      SUM(sent) as sent,
      SUM(purchased) as purchased,
      SUM(earnings) as earnings
    FROM `of-scheduler-proj.mart.v_dm_send_facts_v3`
    WHERE DATE(sending_dt_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
      AND dm_type = 'ppv'  -- PPV only
      AND sent > 0
    GROUP BY username_page, DATE(sending_dt_local)
  )
  GROUP BY username_page
),

page_classification AS (
  SELECT 
    rp.*,
    
    -- Performance score (0-100 scale)
    LEAST(100, GREATEST(0,
      (COALESCE(avg_conversion_rate, 0) * 400) +  -- 40% weight on conversion
      (LEAST(total_revenue_28d / 50, 40)) +       -- 40% weight on revenue (capped)
      (CASE WHEN avg_daily_sends >= 5 THEN 20 ELSE avg_daily_sends * 4 END)  -- 20% weight on volume capacity
    )) as performance_score,
    
    -- Tier classification
    CASE 
      WHEN avg_conversion_rate >= 0.15 AND total_revenue_28d >= 2000 THEN 'high_volume'
      WHEN avg_conversion_rate >= 0.10 AND total_revenue_28d >= 800 THEN 'standard'
      WHEN avg_conversion_rate >= 0.05 AND total_revenue_28d >= 200 THEN 'conservative'  
      WHEN total_sends_28d >= 20 THEN 'developing'
      ELSE 'minimal'
    END as tier,
    
    -- Quality flags
    CASE 
      WHEN avg_conversion_rate < 0.03 THEN 'LOW_CONVERSION'
      WHEN total_sends_28d < 20 THEN 'INSUFFICIENT_DATA' 
      WHEN active_days_28d < 14 THEN 'INCONSISTENT_ACTIVITY'
      WHEN send_variance > avg_daily_sends * 2 THEN 'ERRATIC_VOLUME'
      ELSE 'HEALTHY'
    END as quality_flag
    
  FROM recent_performance rp
  WHERE rp.username_page IS NOT NULL
),

volume_recommendations AS (
  SELECT 
    pc.*,
    
    -- Volume recommendations based on tier and performance
    CASE pc.tier
      WHEN 'high_volume' THEN 
        CASE 
          WHEN performance_score >= 80 THEN STRUCT(9 as min_sends, 12 as max_sends, 10 as optimal_sends)
          WHEN performance_score >= 60 THEN STRUCT(7 as min_sends, 10 as max_sends, 8 as optimal_sends)
          ELSE STRUCT(6 as min_sends, 8 as max_sends, 7 as optimal_sends)
        END
      WHEN 'standard' THEN
        CASE 
          WHEN performance_score >= 70 THEN STRUCT(5 as min_sends, 8 as max_sends, 6 as optimal_sends)
          WHEN performance_score >= 50 THEN STRUCT(4 as min_sends, 6 as max_sends, 5 as optimal_sends)
          ELSE STRUCT(3 as min_sends, 5 as max_sends, 4 as optimal_sends)
        END
      WHEN 'conservative' THEN
        CASE 
          WHEN performance_score >= 60 THEN STRUCT(3 as min_sends, 5 as max_sends, 4 as optimal_sends)
          WHEN performance_score >= 40 THEN STRUCT(2 as min_sends, 4 as max_sends, 3 as optimal_sends)
          ELSE STRUCT(2 as min_sends, 3 as max_sends, 2 as optimal_sends)
        END
      WHEN 'developing' THEN STRUCT(2 as min_sends, 4 as max_sends, 3 as optimal_sends)
      ELSE STRUCT(1 as min_sends, 3 as max_sends, 2 as optimal_sends)  -- minimal
    END as volume_range,
    
    -- Human-readable recommendations  
    CASE pc.tier
      WHEN 'high_volume' THEN 
        CASE 
          WHEN performance_score >= 80 THEN '9-12 daily (peak performer)'
          WHEN performance_score >= 60 THEN '7-10 daily (strong performer)'
          ELSE '6-8 daily (good performer)'
        END
      WHEN 'standard' THEN
        CASE 
          WHEN performance_score >= 70 THEN '5-8 daily (solid performer)'
          WHEN performance_score >= 50 THEN '4-6 daily (average performer)'
          ELSE '3-5 daily (below average)'
        END
      WHEN 'conservative' THEN
        CASE 
          WHEN performance_score >= 60 THEN '3-5 daily (quality focus)'
          WHEN performance_score >= 40 THEN '2-4 daily (careful approach)'
          ELSE '2-3 daily (minimal volume)'
        END
      WHEN 'developing' THEN '2-4 daily (building data)'
      ELSE '1-3 daily (testing phase)'
    END as volume_recommendation_text,
    
    -- Revenue projections
    ROUND(volume_range.optimal_sends * COALESCE(avg_revenue_per_send, 3.0) * 30, 0) as projected_monthly_revenue,
    
    -- Guidance messages
    CASE 
      WHEN quality_flag = 'LOW_CONVERSION' THEN 'Focus on caption quality and timing - consider A/B testing'
      WHEN quality_flag = 'INSUFFICIENT_DATA' THEN 'Start conservative to build performance history'  
      WHEN quality_flag = 'INCONSISTENT_ACTIVITY' THEN 'Establish consistent daily posting schedule'
      WHEN quality_flag = 'ERRATIC_VOLUME' THEN 'Stabilize send volume for better performance tracking'
      WHEN tier = 'high_volume' THEN 'High-performing page - can handle increased volume'
      WHEN tier = 'standard' THEN 'Solid performance - maintain current approach with optimization'
      WHEN tier = 'conservative' THEN 'Focus on quality over quantity'
      ELSE 'Build consistent performance before scaling volume'
    END as optimization_guidance,
    
    CURRENT_TIMESTAMP() as generated_at
    
  FROM page_classification pc
)

SELECT * FROM volume_recommendations;