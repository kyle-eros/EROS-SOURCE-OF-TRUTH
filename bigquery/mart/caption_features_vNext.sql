-- =====================================================
-- CAPTION FEATURES WITH BAYESIAN SMOOTHING v.Next
-- =====================================================
-- Project: of-scheduler-proj
-- Purpose: Calculate normalized features for caption ranking with exploration
-- Contract: Provides all caption performance metrics with smoothing for sparse data
-- Partition: DATE(computation_ts)
-- Cluster: username_page, caption_id
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.mart.caption_features_vNext` AS
WITH 
-- Global priors for Bayesian smoothing
global_priors AS (
  SELECT
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS global_conversion_rate,
    AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS global_rps,
    AVG(SAFE_DIVIDE(viewed, NULLIF(sent, 0))) AS global_open_rate,
    STDDEV(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS global_conversion_std,
    COUNT(DISTINCT caption_id) AS total_captions,
    COUNT(*) AS total_observations
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND sent > 0
),

-- Page-level priors
page_priors AS (
  SELECT
    username_page,
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS page_conversion_rate,
    AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS page_rps,
    AVG(SAFE_DIVIDE(viewed, NULLIF(sent, 0))) AS page_open_rate,
    COUNT(*) AS page_observations
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND sent > 0
  GROUP BY username_page
),

-- Caption performance with time decay
caption_performance AS (
  SELECT
    mf.caption_id,
    mf.username_page,
    
    -- Raw metrics
    SUM(mf.sent) AS total_sent,
    SUM(mf.viewed) AS total_viewed,
    SUM(mf.purchased) AS total_purchased,
    SUM(mf.earnings_usd) AS total_revenue,
    
    -- Time-weighted metrics (recent performance matters more)
    SUM(mf.sent * POW(0.95, DATE_DIFF(CURRENT_DATE(), DATE(mf.sending_ts), DAY))) AS weighted_sent,
    SUM(mf.viewed * POW(0.95, DATE_DIFF(CURRENT_DATE(), DATE(mf.sending_ts), DAY))) AS weighted_viewed,
    SUM(mf.purchased * POW(0.95, DATE_DIFF(CURRENT_DATE(), DATE(mf.sending_ts), DAY))) AS weighted_purchased,
    SUM(mf.earnings_usd * POW(0.95, DATE_DIFF(CURRENT_DATE(), DATE(mf.sending_ts), DAY))) AS weighted_revenue,
    
    -- Recency signals
    MAX(mf.sending_ts) AS last_used_ts,
    COUNT(DISTINCT DATE(mf.sending_ts)) AS days_used,
    COUNT(DISTINCT mf.price_usd) AS price_points_tested,
    
    -- Variance measures for UCB
    STDDEV(SAFE_DIVIDE(mf.purchased, NULLIF(mf.sent, 0))) AS conversion_variance,
    STDDEV(SAFE_DIVIDE(mf.earnings_usd, NULLIF(mf.sent, 0))) AS rps_variance
    
  FROM `of-scheduler-proj.core.message_facts` mf
  WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND mf.sent > 0
    AND mf.caption_id IS NOT NULL
  GROUP BY mf.caption_id, mf.username_page
),

-- Apply Bayesian smoothing
smoothed_metrics AS (
  SELECT
    cp.caption_id,
    cp.username_page,
    cp.total_sent,
    cp.last_used_ts,
    cp.days_used,
    cp.price_points_tested,
    
    -- Bayesian smoothed conversion rate
    -- Formula: (observed + prior * weight) / (observations + weight)
    -- Weight decreases as observations increase
    SAFE_DIVIDE(
      cp.weighted_purchased + gp.global_conversion_rate * LEAST(30, GREATEST(5, 100 - cp.weighted_sent)),
      cp.weighted_sent + LEAST(30, GREATEST(5, 100 - cp.weighted_sent))
    ) AS smoothed_conversion_rate,
    
    -- Bayesian smoothed RPS
    SAFE_DIVIDE(
      cp.weighted_revenue + gp.global_rps * LEAST(30, GREATEST(5, 100 - cp.weighted_sent)),
      cp.weighted_sent + LEAST(30, GREATEST(5, 100 - cp.weighted_sent))
    ) AS smoothed_rps,
    
    -- Bayesian smoothed open rate
    SAFE_DIVIDE(
      cp.weighted_viewed + gp.global_open_rate * LEAST(30, GREATEST(5, 100 - cp.weighted_sent)),
      cp.weighted_sent + LEAST(30, GREATEST(5, 100 - cp.weighted_sent))
    ) AS smoothed_open_rate,
    
    -- Raw rates for comparison
    SAFE_DIVIDE(cp.total_purchased, NULLIF(cp.total_sent, 0)) AS raw_conversion_rate,
    SAFE_DIVIDE(cp.total_revenue, NULLIF(cp.total_sent, 0)) AS raw_rps,
    SAFE_DIVIDE(cp.total_viewed, NULLIF(cp.total_sent, 0)) AS raw_open_rate,
    
    -- Confidence metrics for exploration
    cp.conversion_variance,
    cp.rps_variance,
    
    -- Exploration bonus (UCB-style)
    -- Higher bonus for high variance or low observations
    LEAST(1.0, 
      SQRT(2 * LN(gp.total_observations) / GREATEST(1, cp.weighted_sent)) +
      COALESCE(cp.conversion_variance, 0.1) * 2
    ) AS exploration_bonus
    
  FROM caption_performance cp
  CROSS JOIN global_priors gp
  LEFT JOIN page_priors pp USING (username_page)
),

-- Calculate novelty/fatigue scores
novelty_scores AS (
  SELECT
    sm.*,
    
    -- Days since last use (novelty indicator)
    DATE_DIFF(CURRENT_DATE(), DATE(sm.last_used_ts), DAY) AS days_since_used,
    
    -- Fatigue penalty (exponential decay)
    CASE 
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(sm.last_used_ts), DAY) < 7 THEN
        EXP(-0.5 * (7 - DATE_DIFF(CURRENT_DATE(), DATE(sm.last_used_ts), DAY)))
      ELSE 1.0
    END AS novelty_score,
    
    -- Usage intensity (penalize overuse)
    CASE
      WHEN sm.days_used > 20 THEN 0.7
      WHEN sm.days_used > 10 THEN 0.85
      ELSE 1.0
    END AS usage_intensity_factor
    
  FROM smoothed_metrics sm
),

-- Z-score normalization
normalized_features AS (
  SELECT
    ns.*,
    cd.caption_text,
    cd.caption_hash,
    cd.explicitness,
    cd.category,
    
    -- Z-score normalization for fair comparison
    SAFE_DIVIDE(
      ns.smoothed_conversion_rate - AVG(ns.smoothed_conversion_rate) OVER (PARTITION BY ns.username_page),
      NULLIF(STDDEV(ns.smoothed_conversion_rate) OVER (PARTITION BY ns.username_page), 0)
    ) AS conversion_z_score,
    
    SAFE_DIVIDE(
      ns.smoothed_rps - AVG(ns.smoothed_rps) OVER (PARTITION BY ns.username_page),
      NULLIF(STDDEV(ns.smoothed_rps) OVER (PARTITION BY ns.username_page), 0)
    ) AS rps_z_score,
    
    SAFE_DIVIDE(
      ns.smoothed_open_rate - AVG(ns.smoothed_open_rate) OVER (PARTITION BY ns.username_page),
      NULLIF(STDDEV(ns.smoothed_open_rate) OVER (PARTITION BY ns.username_page), 0)
    ) AS open_z_score,
    
    -- Percentile ranks for interpretability
    PERCENT_RANK() OVER (PARTITION BY ns.username_page ORDER BY ns.smoothed_conversion_rate) AS conversion_percentile,
    PERCENT_RANK() OVER (PARTITION BY ns.username_page ORDER BY ns.smoothed_rps) AS rps_percentile,
    PERCENT_RANK() OVER (PARTITION BY ns.username_page ORDER BY ns.smoothed_open_rate) AS open_percentile
    
  FROM novelty_scores ns
  LEFT JOIN `of-scheduler-proj.core.caption_dim` cd 
    ON ns.caption_id = cd.caption_id
)

-- Final output with all features
SELECT
  nf.caption_id,
  nf.caption_text,
  nf.caption_hash,
  nf.username_page,
  nf.category,
  nf.explicitness,
  
  -- Core performance metrics (smoothed)
  ROUND(nf.smoothed_conversion_rate, 4) AS conversion_rate,
  ROUND(nf.smoothed_rps, 2) AS rps,
  ROUND(nf.smoothed_open_rate, 4) AS open_rate,
  
  -- Normalized scores for ranking
  ROUND(COALESCE(nf.conversion_z_score, 0), 3) AS conversion_z_score,
  ROUND(COALESCE(nf.rps_z_score, 0), 3) AS rps_z_score,
  ROUND(COALESCE(nf.open_z_score, 0), 3) AS open_z_score,
  
  -- Percentile ranks
  ROUND(nf.conversion_percentile, 3) AS conversion_percentile,
  ROUND(nf.rps_percentile, 3) AS rps_percentile,
  ROUND(nf.open_percentile, 3) AS open_percentile,
  
  -- Novelty and exploration
  ROUND(nf.novelty_score, 3) AS novelty_score,
  ROUND(nf.usage_intensity_factor, 3) AS usage_intensity_factor,
  ROUND(nf.exploration_bonus, 3) AS exploration_bonus,
  
  -- Metadata for debugging
  nf.total_sent,
  nf.days_used,
  nf.days_since_used,
  nf.last_used_ts,
  nf.price_points_tested,
  ROUND(nf.conversion_variance, 4) AS conversion_variance,
  ROUND(nf.rps_variance, 2) AS rps_variance,
  
  -- Data quality flags
  CASE WHEN nf.total_sent < 10 THEN TRUE ELSE FALSE END AS is_cold_start,
  CASE WHEN nf.days_since_used > 28 THEN TRUE ELSE FALSE END AS is_stale,
  CURRENT_TIMESTAMP() AS computation_ts
  
FROM normalized_features nf
WHERE nf.caption_id IS NOT NULL
  AND nf.caption_text IS NOT NULL;