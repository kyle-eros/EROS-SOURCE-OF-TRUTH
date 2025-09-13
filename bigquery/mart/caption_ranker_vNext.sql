-- =====================================================
-- CAPTION RANKER WITH UCB EXPLORATION v.Next
-- =====================================================
-- Project: of-scheduler-proj
-- Purpose: Rank captions for each slot using configurable ML weights + exploration
-- Contract: Returns top-N captions per slot with scores, flags, and reason codes
-- Partition: DATE(slot_dt_local)
-- Cluster: username_page, slot_dt_local, hod_local
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.mart.caption_ranker_vNext` AS
WITH 
-- Get ML weights from config
ml_weights AS (
  SELECT 
    w.*,
    ps.page_state
  FROM `of-scheduler-proj.core.page_state` ps
  INNER JOIN `of-scheduler-proj.ops.ml_ranking_weights_v1` w
    ON ps.page_state = w.page_state
  WHERE w.updated_at = (
    SELECT MAX(updated_at) 
    FROM `of-scheduler-proj.ops.ml_ranking_weights_v1`
  )
),

-- Get exploration config
explore_config AS (
  SELECT * 
  FROM `of-scheduler-proj.ops.explore_exploit_config_v1`
  WHERE config_key = 'default'
    AND updated_at = (
      SELECT MAX(updated_at) 
      FROM `of-scheduler-proj.ops.explore_exploit_config_v1`
      WHERE config_key = 'default'
    )
),

-- Get scheduled slots for next 7 days
scheduled_slots AS (
  SELECT
    username_page,
    username_std,
    page_type,
    schedule_date AS slot_dt_local,
    hod_local,
    slot_rank,
    tracking_hash,
    -- Calculate DOW for feature matching
    MOD(EXTRACT(DAYOFWEEK FROM schedule_date) + 5, 7) AS dow_local
  FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`
  WHERE schedule_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
),

-- Get DOW×HOD performance patterns
dow_hod_patterns AS (
  SELECT
    username_std,
    dow_local,
    hod_local,
    score AS dow_hod_score,
    PERCENT_RANK() OVER (PARTITION BY username_std ORDER BY score) AS dow_hod_percentile
  FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`
),

-- Get price elasticity from historical data
price_elasticity AS (
  SELECT
    username_page,
    price_band,
    AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS band_rps,
    COUNT(*) AS band_observations,
    -- Find optimal price point
    FIRST_VALUE(price_band) OVER (
      PARTITION BY username_page 
      ORDER BY AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) DESC
    ) AS optimal_band
  FROM (
    SELECT 
      username_page,
      earnings_usd,
      sent,
      CASE 
        WHEN price_usd < 15 THEN 'low'
        WHEN price_usd < 30 THEN 'mid'
        WHEN price_usd < 45 THEN 'high'
        ELSE 'premium'
      END AS price_band
    FROM `of-scheduler-proj.core.message_facts`
    WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
      AND sent > 0
  )
  GROUP BY username_page, price_band
),

-- Get cooldown violations to exclude
cooldown_check AS (
  SELECT
    mf.caption_id,
    mf.username_page,
    MAX(mf.sending_ts) AS last_sent_ts,
    COUNT(*) AS recent_uses_7d,
    COUNT(DISTINCT DATE(mf.sending_ts)) AS unique_days_7d
  FROM `of-scheduler-proj.core.message_facts` mf
  WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY mf.caption_id, mf.username_page
),

-- Calculate momentum scores
momentum_scores AS (
  SELECT
    username_page,
    -- 7-day vs 30-day performance
    SAFE_DIVIDE(
      SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
               THEN earnings_usd END),
      NULLIF(SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
                      THEN sent END), 0)
    ) AS rps_7d,
    SAFE_DIVIDE(
      SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
               THEN earnings_usd END),
      NULLIF(SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
                      THEN sent END), 0)
    ) AS rps_30d,
    -- Momentum indicator
    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
                 THEN earnings_usd END),
        NULLIF(SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
                        THEN sent END), 0)
      ),
      NULLIF(SAFE_DIVIDE(
        SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
                 THEN earnings_usd END),
        NULLIF(SUM(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
                        THEN sent END), 0)
      ), 0)
    ) AS momentum_ratio
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND sent > 0
  GROUP BY username_page
),

-- Combine all features and calculate scores
scored_captions AS (
  SELECT
    ss.username_page,
    ss.username_std,
    ss.page_type,
    ss.slot_dt_local,
    ss.hod_local,
    ss.dow_local,
    ss.slot_rank,
    ss.tracking_hash,
    cf.caption_id,
    cf.caption_text,
    cf.caption_hash,
    cf.category,
    cf.explicitness,
    
    -- Raw features
    cf.conversion_rate,
    cf.rps,
    cf.open_rate,
    COALESCE(dhp.dow_hod_score, 0) AS dow_hod_score,
    COALESCE(dhp.dow_hod_percentile, 0.5) AS dow_hod_percentile,
    cf.novelty_score,
    COALESCE(ms.momentum_ratio, 1.0) AS momentum_score,
    
    -- Normalized features
    cf.rps_z_score,
    cf.conversion_z_score,
    cf.open_z_score,
    
    -- Get ML weights for this page
    mw.w_rps,
    mw.w_open,
    mw.w_buy,
    mw.w_dowhod,
    mw.w_price,
    mw.w_novelty,
    mw.w_momentum,
    mw.ucb_c,
    mw.epsilon,
    
    -- Exploration bonus
    cf.exploration_bonus,
    ec.max_explorer_share,
    
    -- Calculate final score
    -- score_final = weighted sum of normalized features + exploration
    (
      mw.w_rps * COALESCE(cf.rps_z_score, 0) +
      mw.w_open * COALESCE(cf.open_z_score, 0) +
      mw.w_buy * COALESCE(cf.conversion_z_score, 0) +
      mw.w_dowhod * COALESCE(dhp.dow_hod_percentile - 0.5, 0) * 2 + -- Scale to [-1, 1]
      mw.w_price * CASE 
        WHEN pe.optimal_band = 'mid' AND cf.rps > pe.band_rps THEN 0.2
        WHEN pe.optimal_band = 'high' AND cf.rps > pe.band_rps THEN 0.1
        ELSE 0
      END +
      mw.w_novelty * cf.novelty_score +
      mw.w_momentum * LEAST(1.5, GREATEST(0.5, COALESCE(ms.momentum_ratio, 1.0))) +
      -- UCB exploration bonus
      CASE 
        WHEN cf.is_cold_start THEN mw.ucb_c * cf.exploration_bonus
        WHEN RAND() < mw.epsilon THEN 2.0  -- Epsilon-greedy exploration
        ELSE 0
      END
    ) AS score_final,
    
    -- Compliance flags
    CASE 
      WHEN cc.recent_uses_7d >= 3 THEN FALSE
      WHEN cc.unique_days_7d >= 3 THEN FALSE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(cc.last_sent_ts), HOUR) < 168 THEN FALSE
      ELSE TRUE
    END AS cooldown_ok,
    
    -- Check quota (simplified - would join with quota table)
    TRUE AS quota_ok,
    
    -- Deduplication check (no same caption in 7 days)
    CASE 
      WHEN cc.recent_uses_7d > 0 THEN FALSE
      ELSE TRUE
    END AS dedupe_ok,
    
    -- Explorer flag
    CASE 
      WHEN cf.is_cold_start THEN TRUE
      WHEN RAND() < mw.epsilon THEN TRUE
      ELSE FALSE
    END AS is_explorer,
    
    -- Metadata
    cf.total_sent,
    cf.days_since_used,
    cf.is_cold_start,
    cf.is_stale,
    cc.recent_uses_7d,
    
    -- Reason codes
    CASE
      WHEN cf.is_cold_start THEN 'cold_start_exploration'
      WHEN RAND() < mw.epsilon THEN 'epsilon_exploration'
      WHEN cf.rps_percentile > 0.8 THEN 'high_performer'
      WHEN dhp.dow_hod_percentile > 0.7 THEN 'optimal_timing'
      WHEN cf.novelty_score > 0.9 THEN 'fresh_content'
      ELSE 'balanced_selection'
    END AS selection_reason
    
  FROM scheduled_slots ss
  CROSS JOIN `of-scheduler-proj.mart.caption_features_vNext` cf
  LEFT JOIN ml_weights mw ON ss.username_std = mw.username_std
  LEFT JOIN dow_hod_patterns dhp 
    ON ss.username_std = dhp.username_std 
    AND ss.dow_local = dhp.dow_local 
    AND ss.hod_local = dhp.hod_local
  LEFT JOIN price_elasticity pe ON ss.username_page = pe.username_page
  LEFT JOIN cooldown_check cc 
    ON cf.caption_id = cc.caption_id 
    AND ss.username_page = cc.username_page
  LEFT JOIN momentum_scores ms ON ss.username_page = ms.username_page
  CROSS JOIN explore_config ec
  WHERE cf.username_page = ss.username_page
),

-- Rank captions per slot
ranked_captions AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY username_page, slot_dt_local, hod_local
      ORDER BY 
        -- Compliance first
        CASE WHEN cooldown_ok AND quota_ok AND dedupe_ok THEN 0 ELSE 1 END,
        -- Then score
        score_final DESC,
        -- Tiebreaker: prefer fresher content
        days_since_used DESC
    ) AS rank_in_slot,
    
    -- Calculate diversity within top picks
    COUNT(DISTINCT category) OVER (
      PARTITION BY username_page, slot_dt_local, hod_local
      ORDER BY score_final DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS category_diversity,
    
    -- Normalize score to 0-100 scale
    100 * (score_final - MIN(score_final) OVER (PARTITION BY username_page)) / 
    NULLIF(MAX(score_final) OVER (PARTITION BY username_page) - 
           MIN(score_final) OVER (PARTITION BY username_page), 0) AS score_normalized
    
  FROM scored_captions
  WHERE caption_id IS NOT NULL
    AND caption_text IS NOT NULL
)

-- Final output
SELECT
  username_page,
  username_std,
  page_type,
  slot_dt_local,
  hod_local,
  dow_local,
  slot_rank,
  tracking_hash,
  caption_id,
  caption_text,
  caption_hash,
  category,
  explicitness,
  
  -- Scores
  ROUND(score_final, 3) AS score_final,
  ROUND(score_normalized, 1) AS score_normalized,
  rank_in_slot,
  
  -- Performance metrics
  ROUND(conversion_rate, 4) AS conversion_rate,
  ROUND(rps, 2) AS rps,
  ROUND(open_rate, 4) AS open_rate,
  
  -- Timing fit
  ROUND(dow_hod_score, 2) AS dow_hod_score,
  ROUND(dow_hod_percentile, 3) AS dow_hod_percentile,
  
  -- Novelty and momentum
  ROUND(novelty_score, 3) AS novelty_score,
  ROUND(momentum_score, 3) AS momentum_score,
  
  -- Compliance flags
  cooldown_ok,
  quota_ok,
  dedupe_ok,
  is_explorer,
  
  -- Metadata
  total_sent,
  days_since_used,
  recent_uses_7d,
  is_cold_start,
  is_stale,
  selection_reason,
  category_diversity,
  
  -- Audit fields
  CURRENT_TIMESTAMP() AS ranked_at,
  '{{ deployment_version }}' AS model_version
  
FROM ranked_captions
WHERE rank_in_slot <= 20  -- Keep top 20 per slot for picker
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY username_page, slot_dt_local, hod_local, caption_id
  ORDER BY rank_in_slot
) = 1;  -- Remove any duplicates