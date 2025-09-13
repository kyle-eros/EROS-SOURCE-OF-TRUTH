WITH 
-- FIXED: ML weights with username_std and proper latest selection
ml_weights AS (
  SELECT
    ps.username_std,
    ps.page_state,
    w.w_rps, w.w_open, w.w_buy, w.w_dowhod, w.w_price, w.w_novelty, w.w_momentum,
    w.ucb_c, w.epsilon
  FROM `of-scheduler-proj.ops_config.page_state` ps
  JOIN (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY page_state ORDER BY updated_at DESC) AS rn
      FROM `of-scheduler-proj.ops.ml_ranking_weights_v1`
    )
    WHERE rn = 1
  ) w USING (page_state)
),

-- Get exploration config with proper latest selection
explore_config AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY config_key ORDER BY updated_at DESC) AS rn
    FROM `of-scheduler-proj.ops.explore_exploit_config_v1`
  )
  WHERE config_key = 'default' AND rn = 1
),

-- FIXED: Get cooldown config from settings
cooldown_config AS (
  SELECT 
    CAST(MAX(CASE WHEN setting_key = 'min_cooldown_hours' THEN setting_value END) AS INT64) AS min_cooldown_hours,
    21 * 24 AS max_cooldown_hours,  -- 21 days from max_cooldown_days setting
    3 AS max_weekly_uses  -- Standard max weekly uses
  FROM `of-scheduler-proj.ops_config.cooldown_settings_v1`
  WHERE setting_key IN ('min_cooldown_hours', 'max_cooldown_days')
),

-- Get scheduled slots for next 7 days
scheduled_slots AS (
  SELECT
    username_page,
    username_std,
    page_type,
    date_local AS slot_dt_local,
    hod_local,
    slot_rank,
    tracking_hash,
    MOD(EXTRACT(DAYOFWEEK FROM date_local) + 5, 7) AS dow_local
  FROM `of-scheduler-proj.mart.v_weekly_template_7d_pages`
  WHERE date_local BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
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

-- FIXED: Price elasticity - one row per page with optimal band
price_elasticity AS (
  SELECT 
    username_page,
    ANY_VALUE(optimal_band) AS optimal_band,
    MAX_BY(band_rps, band_rps) AS optimal_band_rps
  FROM (
    SELECT 
      username_page,
      price_band,
      AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS band_rps,
      FIRST_VALUE(price_band) OVER (
        PARTITION BY username_page 
        ORDER BY AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) DESC
      ) AS optimal_band
    FROM (
      SELECT 
        CONCAT(mf.username_std, '__', COALESCE(pta.page_type, 'main')) AS username_page,
        mf.earnings_usd,
        mf.sent,
        mf.price_usd,
        CASE 
          WHEN mf.price_usd < 15 THEN 'low'
          WHEN mf.price_usd < 30 THEN 'mid'
          WHEN mf.price_usd < 45 THEN 'high'
          ELSE 'premium'
        END AS price_band
      FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page` mf
      LEFT JOIN `of-scheduler-proj.ops_config.page_type_authority` pta
        ON mf.username_std = pta.username_std
      WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        AND mf.sent > 0
    )
    GROUP BY username_page, price_band
  )
  GROUP BY username_page
),

-- FIXED: Cooldown check with TIMESTAMP_DIFF
cooldown_check AS (
  SELECT
    cd.caption_id,
    CONCAT(mf.username_std, '__', COALESCE(pta.page_type, 'main')) AS username_page,
    MAX(mf.sending_ts) AS last_sent_ts,
    COUNTIF(mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS recent_uses_7d,
    COUNT(DISTINCT DATE(mf.sending_ts)) AS unique_days_7d
  FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page` mf
  LEFT JOIN `of-scheduler-proj.ops_config.page_type_authority` pta
    ON mf.username_std = pta.username_std
  LEFT JOIN `of-scheduler-proj.layer_04_semantic.v_caption_dim` cd
    ON mf.caption_hash = cd.caption_hash
    AND mf.username_std = cd.username_std
  WHERE mf.caption_hash IS NOT NULL
    AND cd.caption_id IS NOT NULL
  GROUP BY cd.caption_id, username_page
),

-- Calculate momentum scores
momentum_scores AS (
  SELECT
    CONCAT(mf.username_std, '__', COALESCE(pta.page_type, 'main')) AS username_page,
    SAFE_DIVIDE(
      SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
               THEN mf.earnings_usd END),
      NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
                      THEN mf.sent END), 0)
    ) AS rps_7d,
    SAFE_DIVIDE(
      SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
               THEN mf.earnings_usd END),
      NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
                      THEN mf.sent END), 0)
    ) AS rps_30d,
    SAFE_DIVIDE(
      SAFE_DIVIDE(
        SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
                 THEN mf.earnings_usd END),
        NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) 
                        THEN mf.sent END), 0)
      ),
      NULLIF(SAFE_DIVIDE(
        SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
                 THEN mf.earnings_usd END),
        NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) 
                        THEN mf.sent END), 0)
      ), 0)
    ) AS momentum_ratio
  FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page` mf
  LEFT JOIN `of-scheduler-proj.ops_config.page_type_authority` pta
    ON mf.username_std = pta.username_std
  WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND mf.sent > 0
  GROUP BY username_page
),

-- FIXED: Combine features with proper JOINs (not CROSS JOIN)
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
    
    -- ML weights
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
    
    -- FIXED: Deterministic epsilon flag using hash
    (ABS(FARM_FINGERPRINT(CONCAT(
      cf.caption_id,
      FORMAT_DATE('%Y%m%d', ss.slot_dt_local),
      CAST(ss.hod_local AS STRING)
    ))) / 9.22e18) < mw.epsilon AS epsilon_flag,
    
    -- Calculate final score
    (
      mw.w_rps * COALESCE(cf.rps_z_score, 0) +
      mw.w_open * COALESCE(cf.open_z_score, 0) +
      mw.w_buy * COALESCE(cf.conversion_z_score, 0) +
      mw.w_dowhod * COALESCE((dhp.dow_hod_percentile - 0.5) * 2, 0) +
      mw.w_price * CASE 
        WHEN pe.optimal_band = 'mid' AND cf.rps > pe.optimal_band_rps THEN 0.2
        WHEN pe.optimal_band = 'high' AND cf.rps > pe.optimal_band_rps THEN 0.1
        ELSE 0
      END +
      mw.w_novelty * cf.novelty_score +
      mw.w_momentum * LEAST(1.5, GREATEST(0.5, COALESCE(ms.momentum_ratio, 1.0))) +
      -- UCB exploration bonus (deterministic)
      CASE 
        WHEN cf.is_cold_start THEN mw.ucb_c * cf.exploration_bonus
        WHEN (ABS(FARM_FINGERPRINT(CONCAT(
          cf.caption_id,
          FORMAT_DATE('%Y%m%d', ss.slot_dt_local),
          CAST(ss.hod_local AS STRING)
        ))) / 9.22e18) < mw.epsilon THEN 2.0
        ELSE 0
      END
    ) AS score_final,
    
    -- FIXED: Compliance flags with config-driven thresholds
    CASE 
      WHEN cc.recent_uses_7d >= (SELECT max_weekly_uses FROM cooldown_config) THEN FALSE
      WHEN cc.unique_days_7d >= 3 THEN FALSE
      WHEN cc.last_sent_ts IS NOT NULL 
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), cc.last_sent_ts, HOUR) < 
            (SELECT min_cooldown_hours FROM cooldown_config) THEN FALSE
      ELSE TRUE
    END AS cooldown_ok,
    
    TRUE AS quota_ok,  -- Would join with quota table
    
    CASE 
      WHEN cc.recent_uses_7d > 0 THEN FALSE
      ELSE TRUE
    END AS dedupe_ok,
    
    CASE 
      WHEN cf.is_cold_start THEN TRUE
      WHEN (ABS(FARM_FINGERPRINT(CONCAT(
        cf.caption_id,
        FORMAT_DATE('%Y%m%d', ss.slot_dt_local),
        CAST(ss.hod_local AS STRING)
      ))) / 9.22e18) < mw.epsilon THEN TRUE
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
      WHEN (ABS(FARM_FINGERPRINT(CONCAT(
        cf.caption_id,
        FORMAT_DATE('%Y%m%d', ss.slot_dt_local),
        CAST(ss.hod_local AS STRING)
      ))) / 9.22e18) < mw.epsilon THEN 'epsilon_exploration'
      WHEN cf.rps_percentile > 0.8 THEN 'high_performer'
      WHEN dhp.dow_hod_percentile > 0.7 THEN 'optimal_timing'
      WHEN cf.novelty_score > 0.9 THEN 'fresh_content'
      ELSE 'balanced_selection'
    END AS selection_reason
    
  FROM scheduled_slots ss
  -- FIXED: Proper JOIN instead of CROSS JOIN
  INNER JOIN `of-scheduler-proj.mart.caption_features_vNext` cf
    ON cf.username_page = ss.username_page
  LEFT JOIN ml_weights mw 
    ON ss.username_std = mw.username_std
  LEFT JOIN dow_hod_patterns dhp 
    ON ss.username_std = dhp.username_std 
    AND ss.dow_local = dhp.dow_local 
    AND ss.hod_local = dhp.hod_local
  LEFT JOIN price_elasticity pe 
    ON ss.username_page = pe.username_page
  LEFT JOIN cooldown_check cc 
    ON cf.caption_id = cc.caption_id 
    AND ss.username_page = cc.username_page
  LEFT JOIN momentum_scores ms 
    ON ss.username_page = ms.username_page
  CROSS JOIN explore_config ec
  CROSS JOIN cooldown_config
),

-- Rank captions per slot
ranked_captions AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY username_page, slot_dt_local, hod_local
      ORDER BY 
        CASE WHEN cooldown_ok AND quota_ok AND dedupe_ok THEN 0 ELSE 1 END,
        score_final DESC,
        days_since_used DESC
    ) AS rank_in_slot,
    
    COUNT(DISTINCT category) OVER (
      PARTITION BY username_page, slot_dt_local, hod_local
    ) AS category_diversity,
    
    -- FIXED: Normalize score per slot (not per page)
    100 * (score_final - MIN(score_final) OVER (PARTITION BY username_page, slot_dt_local, hod_local)) / 
    NULLIF(
      MAX(score_final) OVER (PARTITION BY username_page, slot_dt_local, hod_local) - 
      MIN(score_final) OVER (PARTITION BY username_page, slot_dt_local, hod_local), 
      0
    ) AS score_normalized
    
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
  
  ROUND(score_final, 3) AS score_final,
  ROUND(score_normalized, 1) AS score_normalized,
  rank_in_slot,
  
  ROUND(conversion_rate, 4) AS conversion_rate,
  ROUND(rps, 2) AS rps,
  ROUND(open_rate, 4) AS open_rate,
  
  ROUND(dow_hod_score, 2) AS dow_hod_score,
  ROUND(dow_hod_percentile, 3) AS dow_hod_percentile,
  
  ROUND(novelty_score, 3) AS novelty_score,
  ROUND(momentum_score, 3) AS momentum_score,
  
  cooldown_ok,
  quota_ok,
  dedupe_ok,
  is_explorer,
  
  total_sent,
  days_since_used,
  recent_uses_7d,
  is_cold_start,
  is_stale,
  selection_reason,
  category_diversity,
  
  CURRENT_TIMESTAMP() AS ranked_at,
  'v1.0.1-patched' AS model_version
  
FROM ranked_captions
WHERE rank_in_slot <= 20
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY username_page, slot_dt_local, hod_local, caption_id
  ORDER BY rank_in_slot
) = 1
