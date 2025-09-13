-- =========================================
-- ML FEATURE STORE
-- =========================================
-- Purpose: Comprehensive feature engineering for ML ranking
-- Includes exploration/exploitation balance, temporal patterns,
-- and Bayesian smoothing for robust predictions
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_05_ml.feature_store`
PARTITION BY DATE(computed_at)
CLUSTER BY username_page, caption_id
AS
WITH base_features AS (
  -- Get latest performance metrics for each caption
  SELECT
    cp.caption_id,
    cp.creator_username,
    cp.creator_page_type,
    cp.username_page,
    cp.caption_category,
    
    -- Recent performance (last 30 days)
    AVG(cp.revenue_per_send) AS rps_30d,
    AVG(cp.purchase_rate) AS purchase_rate_30d,
    SUM(cp.total_messages_sent) AS sends_30d,
    SUM(cp.total_revenue) AS revenue_30d,
    COUNT(DISTINCT cp.metric_date) AS active_days_30d,
    
    -- Trend metrics
    AVG(cp.rps_trend_slope) AS trend_slope,
    AVG(cp.stability_score) AS stability_score,
    
    -- Time-decayed metrics (exponential decay with 14-day half-life)
    SUM(cp.revenue_per_send * EXP(-0.0495 * DATE_DIFF(CURRENT_DATE(), cp.metric_date, DAY))) / 
    SUM(EXP(-0.0495 * DATE_DIFF(CURRENT_DATE(), cp.metric_date, DAY))) AS rps_weighted,
    
    -- Recency
    MAX(cp.metric_date) AS last_used_date,
    DATE_DIFF(CURRENT_DATE(), MAX(cp.metric_date), DAY) AS days_since_use,
    
    -- Statistical distribution
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(50)] AS median_rps,
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(75)] AS p75_rps,
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(25)] AS p25_rps,
    STDDEV(cp.revenue_per_send) AS rps_stddev
    
  FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily` cp
  WHERE cp.metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1,2,3,4,5
),

bayesian_smoothing AS (
  -- Apply Bayesian smoothing for sparse data
  SELECT
    bf.*,
    
    -- Global priors (calculated from all data)
    AVG(rps_30d) OVER() AS global_avg_rps,
    AVG(purchase_rate_30d) OVER() AS global_avg_purchase_rate,
    PERCENTILE_CONT(sends_30d, 0.5) OVER() AS global_median_sends,
    
    -- Category-level priors
    AVG(rps_30d) OVER(PARTITION BY caption_category) AS category_avg_rps,
    AVG(purchase_rate_30d) OVER(PARTITION BY caption_category) AS category_avg_purchase_rate,
    
    -- Smoothing weight (more weight on observed data as sample size increases)
    sends_30d / (sends_30d + 100) AS smoothing_weight
    
  FROM base_features bf
),

smoothed_metrics AS (
  SELECT
    *,
    
    -- Bayesian smoothed metrics
    (rps_30d * sends_30d + category_avg_rps * 100) / (sends_30d + 100) AS rps_smoothed,
    (purchase_rate_30d * sends_30d + category_avg_purchase_rate * 100) / (sends_30d + 100) AS purchase_rate_smoothed,
    
    -- Confidence score based on sample size and stability
    LEAST(1.0, sends_30d / 1000) * stability_score AS confidence_score
    
  FROM bayesian_smoothing
),

exploration_features AS (
  -- Calculate exploration bonuses
  SELECT
    sm.*,
    
    -- Upper Confidence Bound (UCB) for exploration
    SQRT(2 * LN(1 + total_sends.total) / GREATEST(1, sends_30d)) AS ucb_bonus,
    
    -- Temperature for softmax exploration (higher for newer captions)
    1 / (1 + EXP(-0.1 * (30 - LEAST(30, days_since_use)))) AS exploration_temperature,
    
    -- Novelty score (bonus for underexplored captions)
    CASE
      WHEN sends_30d < 10 THEN 1.0
      WHEN sends_30d < 50 THEN 0.5
      WHEN sends_30d < 100 THEN 0.2
      ELSE 0.0
    END AS novelty_bonus
    
  FROM smoothed_metrics sm
  CROSS JOIN (
    SELECT SUM(sends_30d) AS total
    FROM base_features
  ) AS total_sends
),

temporal_features AS (
  -- Extract temporal patterns
  SELECT
    ef.*,
    
    -- Best performing time slots
    (
      SELECT AS STRUCT
        hour,
        AVG(rps) AS avg_rps
      FROM (
        SELECT 
          h.hour,
          h.rps
        FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily` cp,
        UNNEST(cp.hourly_performance) h
        WHERE cp.caption_id = ef.caption_id
          AND cp.metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      )
      GROUP BY hour
      ORDER BY avg_rps DESC
      LIMIT 1
    ).hour AS best_hour,
    
    (
      SELECT AS STRUCT
        hour,
        AVG(rps) AS avg_rps
      FROM (
        SELECT 
          h.hour,
          h.rps
        FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily` cp,
        UNNEST(cp.hourly_performance) h
        WHERE cp.caption_id = ef.caption_id
          AND cp.metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      )
      GROUP BY hour
      ORDER BY avg_rps DESC
      LIMIT 1
    ).avg_rps AS best_hour_rps,
    
    -- Day of week performance
    (
      SELECT 
        ARRAY_AGG(
          STRUCT(dow, avg_rps)
          ORDER BY avg_rps DESC
        )[OFFSET(0)].dow
      FROM (
        SELECT 
          EXTRACT(DAYOFWEEK FROM metric_date) AS dow,
          AVG(revenue_per_send) AS avg_rps
        FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily`
        WHERE caption_id = ef.caption_id
          AND metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY dow
      )
    ) AS best_day_of_week
    
  FROM exploration_features ef
),

cooldown_features AS (
  -- Calculate dynamic cooldowns
  SELECT
    tf.*,
    
    -- Base cooldown (exponential backoff)
    GREATEST(
      6,  -- Minimum 6 hours
      6 * POW(1.5, LEAST(5, active_days_30d / 7))
    ) AS base_cooldown_hours,
    
    -- Fatigue score (0-1, higher means more fatigued)
    1 / (1 + EXP(-0.5 * (active_days_30d - 15))) AS fatigue_score,
    
    -- Is eligible for scheduling
    CASE
      WHEN days_since_use * 24 < 6 THEN FALSE  -- Hard minimum 6 hours
      WHEN 1 / (1 + EXP(-0.5 * (active_days_30d - 15))) > 0.8 THEN FALSE  -- Too fatigued
      WHEN sends_30d < 10 THEN TRUE            -- Always explore new
      ELSE days_since_use * 24 > GREATEST(6, 6 * POW(1.5, LEAST(5, active_days_30d / 7)))
    END AS is_eligible
    
  FROM temporal_features tf
)

SELECT
  -- Identifiers
  caption_id,
  creator_username,
  creator_page_type,
  username_page,
  caption_category,
  
  -- Performance features
  STRUCT(
    rps_30d,
    rps_weighted,
    rps_smoothed,
    purchase_rate_30d,
    purchase_rate_smoothed,
    revenue_30d,
    sends_30d,
    trend_slope,
    stability_score,
    confidence_score
  ) AS performance_features,
  
  -- Statistical features
  STRUCT(
    median_rps,
    p75_rps,
    p25_rps,
    rps_stddev,
    p75_rps - p25_rps AS iqr,
    SAFE_DIVIDE(rps_stddev, NULLIF(median_rps, 0)) AS coefficient_variation
  ) AS statistical_features,
  
  -- Exploration features
  STRUCT(
    ucb_bonus,
    exploration_temperature,
    novelty_bonus,
    smoothing_weight
  ) AS exploration_features,
  
  -- Temporal features
  STRUCT(
    best_hour,
    best_hour_rps,
    best_day_of_week,
    days_since_use,
    last_used_date,
    active_days_30d
  ) AS temporal_features,
  
  -- Cooldown features
  STRUCT(
    base_cooldown_hours,
    fatigue_score,
    is_eligible
  ) AS cooldown_features,
  
  -- Composite scores
  STRUCT(
    -- Base score (weighted combination)
    0.4 * (rps_smoothed / NULLIF(global_avg_rps, 0)) +
    0.3 * stability_score +
    0.2 * (1 - fatigue_score) +
    0.1 * confidence_score AS base_score,
    
    -- Exploration-adjusted score
    (0.4 * (rps_smoothed / NULLIF(global_avg_rps, 0)) +
     0.3 * stability_score +
     0.2 * (1 - fatigue_score) +
     0.1 * confidence_score) * (1 + 0.3 * ucb_bonus) AS exploration_score,
    
    -- Normalized percentile scores
    PERCENT_RANK() OVER (PARTITION BY username_page ORDER BY rps_smoothed) AS rps_percentile,
    PERCENT_RANK() OVER (PARTITION BY caption_category ORDER BY rps_smoothed) AS category_percentile
  ) AS composite_scores,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS computed_at,
  DATE(CURRENT_TIMESTAMP()) AS computed_date
  
FROM cooldown_features;