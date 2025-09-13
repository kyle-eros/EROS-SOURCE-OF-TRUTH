-- =========================================
-- SEMANTIC: Caption Performance Daily
-- =========================================
-- Purpose: Daily aggregated performance metrics for captions
-- Business logic for understanding caption effectiveness
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_04_semantic.caption_performance_daily`
PARTITION BY metric_date
CLUSTER BY creator_username, caption_id
AS
WITH daily_metrics AS (
  -- Aggregate daily performance from fact table
  SELECT
    f.send_date AS metric_date,
    dc.caption_id,
    dc.caption_text,
    dc.caption_category,
    dc.creator_username,
    dc.creator_page_type,
    dc.username_page,
    
    -- Volume metrics
    COUNT(*) AS send_count,
    SUM(f.messages_sent) AS total_messages_sent,
    SUM(f.messages_viewed) AS total_messages_viewed,
    SUM(f.messages_purchased) AS total_messages_purchased,
    
    -- Financial metrics
    SUM(f.gross_revenue_usd) AS total_revenue,
    AVG(f.price_usd) AS avg_price,
    
    -- Performance rates
    SAFE_DIVIDE(SUM(f.messages_viewed), SUM(f.messages_sent)) AS view_rate,
    SAFE_DIVIDE(SUM(f.messages_purchased), SUM(f.messages_sent)) AS purchase_rate,
    SAFE_DIVIDE(SUM(f.net_revenue_usd), SUM(f.messages_sent)) AS revenue_per_send,
    
    -- Percentile metrics for stability analysis
    APPROX_QUANTILES(f.revenue_per_send, 100)[OFFSET(50)] AS median_rps,
    APPROX_QUANTILES(f.revenue_per_send, 100)[OFFSET(75)] AS p75_rps,
    APPROX_QUANTILES(f.revenue_per_send, 100)[OFFSET(25)] AS p25_rps,
    
    -- Time pattern analysis
    ARRAY_AGG(
      STRUCT(
        f.time_of_day_utc AS hour,
        f.messages_sent AS sends,
        f.revenue_per_send AS rps
      ) 
      ORDER BY f.time_of_day_utc
    ) AS hourly_performance
    
  FROM `of-scheduler-proj.layer_03_foundation.fact_message_send` f
  JOIN `of-scheduler-proj.layer_03_foundation.dim_caption` dc
    ON f.caption_key = dc.caption_key
  WHERE f.quality_flag = 'valid'
    AND f.send_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
  GROUP BY 1,2,3,4,5,6,7
),

rolling_metrics AS (
  -- Calculate rolling window metrics
  SELECT
    *,
    
    -- 7-day rolling averages
    AVG(revenue_per_send) OVER (
      PARTITION BY caption_id 
      ORDER BY metric_date 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rps_7d_avg,
    
    AVG(purchase_rate) OVER (
      PARTITION BY caption_id 
      ORDER BY metric_date 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS purchase_rate_7d_avg,
    
    -- 30-day rolling averages
    AVG(revenue_per_send) OVER (
      PARTITION BY caption_id 
      ORDER BY metric_date 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rps_30d_avg,
    
    -- Trend calculation (simplified - positive means improving, negative means declining)
    COALESCE(
      SAFE_DIVIDE(
        AVG(revenue_per_send) OVER (
          PARTITION BY caption_id 
          ORDER BY metric_date 
          ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) - 
        AVG(revenue_per_send) OVER (
          PARTITION BY caption_id 
          ORDER BY metric_date 
          ROWS BETWEEN 29 PRECEDING AND 15 PRECEDING
        ),
        NULLIF(AVG(revenue_per_send) OVER (
          PARTITION BY caption_id 
          ORDER BY metric_date 
          ROWS BETWEEN 29 PRECEDING AND 15 PRECEDING
        ), 0)
      ), 0
    ) AS rps_trend_slope
    
  FROM daily_metrics
  WINDOW w30 AS (
    PARTITION BY caption_id 
    ORDER BY metric_date 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  )
),

performance_scoring AS (
  -- Add performance scores and rankings
  SELECT
    *,
    
    -- Stability score (inverse of coefficient of variation)
    CASE 
      WHEN rps_30d_avg > 0 THEN 
        1 - LEAST(1, ABS(p75_rps - p25_rps) / (2 * rps_30d_avg))
      ELSE 0 
    END AS stability_score,
    
    -- Performance percentiles within creator
    PERCENT_RANK() OVER (
      PARTITION BY creator_username, metric_date 
      ORDER BY revenue_per_send
    ) AS rps_percentile_by_creator,
    
    -- Performance percentiles within category
    PERCENT_RANK() OVER (
      PARTITION BY caption_category, metric_date 
      ORDER BY revenue_per_send
    ) AS rps_percentile_by_category,
    
    -- Days since last use
    DATE_DIFF(
      CURRENT_DATE(),
      MAX(metric_date) OVER (PARTITION BY caption_id),
      DAY
    ) AS days_since_last_use
    
  FROM rolling_metrics
)

SELECT
  -- Identifiers
  metric_date,
  caption_id,
  caption_text,
  caption_category,
  creator_username,
  creator_page_type,
  username_page,
  
  -- Daily metrics
  send_count,
  total_messages_sent,
  total_messages_viewed,
  total_messages_purchased,
  total_revenue,
  avg_price,
  
  -- Performance rates
  view_rate,
  purchase_rate,
  revenue_per_send,
  
  -- Statistical metrics
  median_rps,
  p75_rps,
  p25_rps,
  
  -- Rolling window metrics
  rps_7d_avg,
  purchase_rate_7d_avg,
  rps_30d_avg,
  rps_trend_slope,
  
  -- Scoring
  stability_score,
  rps_percentile_by_creator,
  rps_percentile_by_category,
  
  -- Temporal
  days_since_last_use,
  hourly_performance,
  
  -- Performance tier
  CASE
    WHEN rps_percentile_by_category >= 0.9 THEN 'top_performer'
    WHEN rps_percentile_by_category >= 0.7 THEN 'strong_performer'
    WHEN rps_percentile_by_category >= 0.3 THEN 'average_performer'
    ELSE 'under_performer'
  END AS performance_tier,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS computed_at
  
FROM performance_scoring;