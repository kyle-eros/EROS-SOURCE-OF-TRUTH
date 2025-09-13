-- =========================================
-- ML FEATURE STORE (PRODUCTION VERSION)
-- =========================================
-- Purpose: Comprehensive feature engineering for ML ranking
-- Production-ready with incremental updates, volume-weighted metrics,
-- and temporal features
-- =========================================

-- STEP 1: Create the table schema once (run this only on initial setup)
-- Uncomment and run once:
/*
CREATE TABLE IF NOT EXISTS `of-scheduler-proj.layer_05_ml.feature_store` (
  caption_id STRING,
  creator_username STRING,
  creator_page_type STRING,
  username_page STRING,
  caption_category STRING,
  performance_features STRUCT<
    rps_30d FLOAT64,
    rps_weighted FLOAT64,
    rps_smoothed FLOAT64,
    purchase_rate_30d FLOAT64,
    purchase_rate_smoothed FLOAT64,
    revenue_30d FLOAT64,
    sends_30d INT64,
    trend_slope FLOAT64,
    stability_score FLOAT64,
    confidence_score FLOAT64
  >,
  statistical_features STRUCT<
    median_rps FLOAT64,
    p75_rps FLOAT64,
    p25_rps FLOAT64,
    rps_stddev FLOAT64,
    iqr FLOAT64,
    coefficient_variation FLOAT64
  >,
  exploration_features STRUCT<
    ucb_bonus FLOAT64,
    exploration_temperature FLOAT64,
    novelty_bonus FLOAT64,
    smoothing_weight FLOAT64
  >,
  temporal_features STRUCT<
    best_hour INT64,
    best_hour_rps FLOAT64,
    best_day_of_week INT64,
    hours_since_use INT64,
    last_used_timestamp TIMESTAMP,
    active_days_30d INT64
  >,
  cooldown_features STRUCT<
    base_cooldown_hours FLOAT64,
    fatigue_score FLOAT64,
    is_eligible BOOL
  >,
  composite_scores STRUCT<
    base_score FLOAT64,
    exploration_score FLOAT64,
    rps_percentile FLOAT64,
    category_percentile FLOAT64
  >,
  computed_at TIMESTAMP,
  computed_date DATE
)
PARTITION BY computed_date
CLUSTER BY username_page, caption_id;
*/

-- STEP 2: Daily incremental update (run this daily)

-- Delete today's partition if it exists
DELETE FROM `of-scheduler-proj.layer_05_ml.feature_store`
WHERE computed_date = CURRENT_DATE();

-- Insert new features for today
INSERT INTO `of-scheduler-proj.layer_05_ml.feature_store`
WITH base_features AS (
  -- Get latest performance metrics with VOLUME-WEIGHTED averages
  SELECT
    cp.caption_id,
    cp.creator_username,
    cp.creator_page_type,
    cp.username_page,
    cp.caption_category,
    
    -- Volume-weighted metrics (ratio of sums, not average of ratios)
    SAFE_DIVIDE(SUM(cp.total_revenue), SUM(cp.total_messages_sent)) AS rps_30d,
    SAFE_DIVIDE(SUM(cp.total_messages_purchased), SUM(cp.total_messages_sent)) AS purchase_rate_30d,
    
    -- Time-decayed weighted RPS (half-life = 14 days, explicit)
    SAFE_DIVIDE(
      SUM(cp.total_revenue * cp.total_messages_sent * 
          EXP(-LN(2) * DATE_DIFF(CURRENT_DATE(), cp.metric_date, DAY) / 14.0)),
      SUM(cp.total_messages_sent * 
          EXP(-LN(2) * DATE_DIFF(CURRENT_DATE(), cp.metric_date, DAY) / 14.0))
    ) AS rps_weighted,
    
    -- Volume metrics
    SUM(cp.total_messages_sent) AS sends_30d,
    SUM(cp.total_revenue) AS revenue_30d,
    COUNT(DISTINCT cp.metric_date) AS active_days_30d,
    
    -- Trend metrics
    AVG(cp.rps_trend_slope) AS trend_slope,
    AVG(cp.stability_score) AS stability_score,
    
    -- Statistical distribution (for IQR and stability)
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(50)] AS median_rps,
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(75)] AS p75_rps,
    APPROX_QUANTILES(cp.revenue_per_send, 100)[OFFSET(25)] AS p25_rps,
    STDDEV(cp.revenue_per_send) AS rps_stddev
    
  FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily` cp
  WHERE cp.metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1,2,3,4,5
),

-- Get precise last send times from fact table
last_send_times AS (
  SELECT
    dc.caption_id,
    MAX(f.send_timestamp) AS last_send_timestamp,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(f.send_timestamp), HOUR) AS hours_since_use
  FROM `of-scheduler-proj.layer_03_foundation.fact_message_send` f
  JOIN `of-scheduler-proj.layer_03_foundation.dim_caption` dc
    ON f.caption_key = dc.caption_key
  WHERE f.send_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY 1
),

-- Best hour per caption (no correlation issues)
hourly_performance AS (
  SELECT
    cp.caption_id,
    h.hour AS hour_utc,
    AVG(h.rps) AS avg_rps_hour
  FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily` cp,
       UNNEST(cp.hourly_performance) AS h
  WHERE cp.metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1, 2
),

best_hour AS (
  SELECT 
    caption_id, 
    hour_utc AS best_hour, 
    avg_rps_hour AS best_hour_rps
  FROM (
    SELECT 
      hp.*,
      ROW_NUMBER() OVER (PARTITION BY caption_id ORDER BY avg_rps_hour DESC) AS rn
    FROM hourly_performance hp
  )
  WHERE rn = 1
),

-- Best day of week per caption
dow_performance AS (
  SELECT
    cp.caption_id,
    EXTRACT(DAYOFWEEK FROM cp.metric_date) AS dow,
    AVG(cp.revenue_per_send) AS avg_rps_dow
  FROM `of-scheduler-proj.layer_04_semantic.caption_performance_daily` cp
  WHERE cp.metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1, 2
),

best_dow AS (
  SELECT 
    caption_id, 
    dow AS best_day_of_week
  FROM (
    SELECT 
      dp.*,
      ROW_NUMBER() OVER (PARTITION BY caption_id ORDER BY avg_rps_dow DESC) AS rn
    FROM dow_performance dp
  )
  WHERE rn = 1
),

-- Global statistics
global_stats AS (
  SELECT
    AVG(rps_30d) AS global_avg_rps,
    AVG(purchase_rate_30d) AS global_avg_purchase_rate,
    APPROX_QUANTILES(sends_30d, 100)[OFFSET(50)] AS global_median_sends,
    SUM(sends_30d) AS total_sends
  FROM base_features
),

-- Category-level statistics
category_stats AS (
  SELECT
    caption_category,
    AVG(rps_30d) AS category_avg_rps,
    AVG(purchase_rate_30d) AS category_avg_purchase_rate
  FROM base_features
  GROUP BY caption_category
),

-- Read configuration (will be from ops_config later)
config AS (
  SELECT
    100 AS smoothing_constant,      -- Bayesian prior weight
    2.0 AS ucb_c,                   -- UCB exploration constant
    14.0 AS decay_halflife_days,    -- Time decay half-life
    6 AS min_cooldown_hours,        -- Minimum cooldown
    0.8 AS fatigue_threshold        -- Max fatigue score
),

-- Combine all features
enriched_features AS (
  SELECT
    bf.*,
    lst.last_send_timestamp,
    lst.hours_since_use,
    bh.best_hour,
    bh.best_hour_rps,
    bd.best_day_of_week,
    gs.global_avg_rps,
    gs.global_avg_purchase_rate,
    gs.global_median_sends,
    gs.total_sends,
    cs.category_avg_rps,
    cs.category_avg_purchase_rate,
    cfg.smoothing_constant,
    cfg.ucb_c,
    cfg.min_cooldown_hours,
    cfg.fatigue_threshold,
    
    -- Bayesian smoothed metrics with category fallback
    SAFE_DIVIDE(
      bf.rps_30d * bf.sends_30d + 
      COALESCE(cs.category_avg_rps, gs.global_avg_rps) * cfg.smoothing_constant,
      bf.sends_30d + cfg.smoothing_constant
    ) AS rps_smoothed,
    
    SAFE_DIVIDE(
      bf.purchase_rate_30d * bf.sends_30d + 
      COALESCE(cs.category_avg_purchase_rate, gs.global_avg_purchase_rate) * cfg.smoothing_constant,
      bf.sends_30d + cfg.smoothing_constant
    ) AS purchase_rate_smoothed,
    
    -- Confidence score based on sample size and stability
    LEAST(1.0, bf.sends_30d / 1000) * COALESCE(bf.stability_score, 0.5) AS confidence_score,
    
    -- Upper Confidence Bound (UCB) for exploration
    cfg.ucb_c * SQRT(2 * LN(1 + gs.total_sends) / GREATEST(1, bf.sends_30d)) AS ucb_bonus,
    
    -- Temperature for softmax exploration
    1 / (1 + EXP(-0.1 * (30 - LEAST(30, COALESCE(lst.hours_since_use / 24, 30))))) AS exploration_temperature,
    
    -- Novelty score
    CASE
      WHEN bf.sends_30d < 10 THEN 1.0
      WHEN bf.sends_30d < 50 THEN 0.5
      WHEN bf.sends_30d < 100 THEN 0.2
      ELSE 0.0
    END AS novelty_bonus,
    
    -- Dynamic cooldown
    GREATEST(
      cfg.min_cooldown_hours,
      cfg.min_cooldown_hours * POW(1.5, LEAST(5, bf.active_days_30d / 7))
    ) AS base_cooldown_hours,
    
    -- Fatigue score
    1 / (1 + EXP(-0.5 * (bf.active_days_30d - 15))) AS fatigue_score
    
  FROM base_features bf
  LEFT JOIN last_send_times lst ON bf.caption_id = lst.caption_id
  LEFT JOIN best_hour bh ON bf.caption_id = bh.caption_id
  LEFT JOIN best_dow bd ON bf.caption_id = bd.caption_id
  LEFT JOIN category_stats cs ON bf.caption_category = cs.caption_category
  CROSS JOIN global_stats gs
  CROSS JOIN config cfg
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
    COALESCE(rps_30d, 0) AS rps_30d,
    COALESCE(rps_weighted, rps_30d, 0) AS rps_weighted,
    COALESCE(rps_smoothed, rps_30d, 0) AS rps_smoothed,
    COALESCE(purchase_rate_30d, 0) AS purchase_rate_30d,
    COALESCE(purchase_rate_smoothed, purchase_rate_30d, 0) AS purchase_rate_smoothed,
    COALESCE(revenue_30d, 0) AS revenue_30d,
    COALESCE(sends_30d, 0) AS sends_30d,
    COALESCE(trend_slope, 0) AS trend_slope,
    COALESCE(stability_score, 0.5) AS stability_score,
    COALESCE(confidence_score, 0) AS confidence_score
  ) AS performance_features,
  
  -- Statistical features
  STRUCT(
    COALESCE(median_rps, 0) AS median_rps,
    COALESCE(p75_rps, 0) AS p75_rps,
    COALESCE(p25_rps, 0) AS p25_rps,
    COALESCE(rps_stddev, 0) AS rps_stddev,
    COALESCE(p75_rps - p25_rps, 0) AS iqr,
    SAFE_DIVIDE(rps_stddev, NULLIF(median_rps, 0)) AS coefficient_variation
  ) AS statistical_features,
  
  -- Exploration features
  STRUCT(
    COALESCE(ucb_bonus, 1.0) AS ucb_bonus,
    COALESCE(exploration_temperature, 0.5) AS exploration_temperature,
    COALESCE(novelty_bonus, 0) AS novelty_bonus,
    sends_30d / (sends_30d + smoothing_constant) AS smoothing_weight
  ) AS exploration_features,
  
  -- Temporal features (with actual best hour/dow)
  STRUCT(
    COALESCE(best_hour, 12) AS best_hour,
    COALESCE(best_hour_rps, rps_30d) AS best_hour_rps,
    COALESCE(best_day_of_week, 4) AS best_day_of_week,
    COALESCE(hours_since_use, 999) AS hours_since_use,
    last_send_timestamp,
    active_days_30d
  ) AS temporal_features,
  
  -- Cooldown features (using precise hours)
  STRUCT(
    base_cooldown_hours,
    fatigue_score,
    CASE
      WHEN COALESCE(hours_since_use, 999) < min_cooldown_hours THEN FALSE
      WHEN fatigue_score > fatigue_threshold THEN FALSE
      WHEN sends_30d < 10 THEN TRUE  -- Always explore new
      ELSE COALESCE(hours_since_use, 999) > base_cooldown_hours
    END AS is_eligible
  ) AS cooldown_features,
  
  -- Composite scores
  STRUCT(
    -- Base score (weighted combination)
    0.4 * SAFE_DIVIDE(rps_smoothed, NULLIF(global_avg_rps, 0)) +
    0.3 * COALESCE(stability_score, 0.5) +
    0.2 * (1 - fatigue_score) +
    0.1 * confidence_score AS base_score,
    
    -- Exploration-adjusted score
    (0.4 * SAFE_DIVIDE(rps_smoothed, NULLIF(global_avg_rps, 0)) +
     0.3 * COALESCE(stability_score, 0.5) +
     0.2 * (1 - fatigue_score) +
     0.1 * confidence_score) * (1 + 0.3 * ucb_bonus) AS exploration_score,
    
    -- Normalized percentile scores
    PERCENT_RANK() OVER (PARTITION BY username_page ORDER BY rps_smoothed) AS rps_percentile,
    PERCENT_RANK() OVER (PARTITION BY caption_category ORDER BY rps_smoothed) AS category_percentile
  ) AS composite_scores,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS computed_at,
  CURRENT_DATE() AS computed_date
  
FROM enriched_features;