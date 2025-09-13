-- =====================================================
-- SHEETS SCHEDULE EXPORT WITH COMPLETE CONTRACT v.Next [PATCHED]
-- =====================================================
-- Fixes: price_config join, scheduler_name for RLS, 
-- deterministic jitter, ToS filter, canonical ladder position
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.mart.sheets_schedule_export_vNext` AS
WITH 
-- FIXED: Price bands with proper join
price_config AS (
  SELECT 
    pt.username_std,
    pt.username_page,
    pt.page_type,
    pb.tier,
    pb.min_price,
    pb.max_price,
    pb.optimal_price,
    pb.ladder_steps,
    pb.ladder_increment
  FROM (
    SELECT 
      username_std,
      username_page,
      SPLIT(username_page, '__')[SAFE_OFFSET(1)] AS page_type,
      COALESCE(tier_final, 'standard') AS tier_final
    FROM `of-scheduler-proj.core.v_pages` v
    LEFT JOIN `of-scheduler-proj.core.v_page_tier_final_v1` t 
      USING (username_std)
  ) pt
  JOIN (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY tier, page_type ORDER BY updated_at DESC) AS rn
      FROM `of-scheduler-proj.ops.price_bands_v1`
    )
    WHERE rn = 1
  ) pb
    ON pb.tier = pt.tier_final 
    AND pb.page_type = pt.page_type
),

-- Get fallback configuration
fallback_config AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY page_type, reason_code ORDER BY updated_at DESC) AS rn
    FROM `of-scheduler-proj.ops.fallback_config_v1`
  )
  WHERE rn = 1
),

-- Get scheduler assignments for RLS
scheduler_assignments AS (
  SELECT
    username_std,
    username_page,
    scheduler_name,
    scheduler_email
  FROM `of-scheduler-proj.ops.scheduler_assignments_v1`
  WHERE is_active = TRUE
),

-- ToS/Compliance filter
compliance_filter AS (
  SELECT
    caption_id,
    caption_text,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(banned_word1|banned_word2|illegal)') THEN FALSE
      WHEN explicitness > 0.9 AND page_type = 'main' THEN FALSE  -- Too explicit for main
      ELSE TRUE
    END AS is_compliant
  FROM `of-scheduler-proj.core.caption_dim`
),

-- Get top ranked caption per slot
best_captions AS (
  SELECT
    cr.*,
    cf.is_compliant,
    ROW_NUMBER() OVER (
      PARTITION BY username_page, slot_dt_local, hod_local
      ORDER BY 
        -- Prioritize compliant captions
        CASE WHEN cf.is_compliant THEN 0 ELSE 1 END,
        CASE WHEN cooldown_ok AND quota_ok AND dedupe_ok THEN 0 ELSE 1 END,
        score_final DESC
    ) AS final_rank
  FROM `of-scheduler-proj.mart.caption_ranker_vNext` cr
  LEFT JOIN compliance_filter cf 
    ON cr.caption_id = cf.caption_id
  WHERE rank_in_slot = 1  -- Already the best per slot from ranker
),

-- Generate price ladders
price_ladders AS (
  SELECT
    bc.username_page,
    bc.username_std,
    bc.page_type,
    bc.slot_dt_local,
    bc.hod_local,
    pc.optimal_price AS base_price,
    -- Generate 3-step ladder
    [
      STRUCT(
        ROUND(pc.optimal_price - pc.ladder_increment, 2) AS price,
        1 AS ladder_position
      ),
      STRUCT(
        ROUND(pc.optimal_price, 2) AS price,
        2 AS ladder_position
      ),
      STRUCT(
        ROUND(pc.optimal_price + pc.ladder_increment, 2) AS price,
        3 AS ladder_position  -- Canonical position for QA
      ),
      STRUCT(
        ROUND(pc.optimal_price + pc.ladder_increment * 2, 2) AS price,
        4 AS ladder_position
      ),
      STRUCT(
        ROUND(pc.optimal_price - pc.ladder_increment * 2, 2) AS price,
        5 AS ladder_position
      )
    ] AS price_ladder_array,
    TO_JSON_STRING([
      ROUND(pc.optimal_price - pc.ladder_increment * 2, 2),
      ROUND(pc.optimal_price - pc.ladder_increment, 2),
      ROUND(pc.optimal_price, 2),
      ROUND(pc.optimal_price + pc.ladder_increment, 2),
      ROUND(pc.optimal_price + pc.ladder_increment * 2, 2)
    ]) AS price_ladder_json
  FROM best_captions bc
  LEFT JOIN price_config pc 
    ON bc.username_std = pc.username_std 
    AND bc.page_type = pc.page_type
),

-- Handle slots with no eligible captions (fallback)
final_schedule AS (
  SELECT
    bc.username_page,
    bc.username_std,
    bc.page_type,
    bc.slot_dt_local AS schedule_date,
    bc.hod_local,
    bc.dow_local,
    bc.slot_rank,
    bc.tracking_hash,
    sa.scheduler_name,  -- Added for RLS
    
    -- Caption selection with compliance check
    COALESCE(
      CASE 
        WHEN bc.is_compliant AND bc.cooldown_ok AND bc.quota_ok AND bc.dedupe_ok 
        THEN bc.caption_id
        ELSE NULL
      END,
      fc.fallback_caption_id
    ) AS caption_id,
    
    COALESCE(
      CASE 
        WHEN bc.is_compliant AND bc.cooldown_ok AND bc.quota_ok AND bc.dedupe_ok 
        THEN bc.caption_text
        ELSE NULL
      END,
      fc.fallback_caption_text
    ) AS caption_text,
    
    COALESCE(bc.caption_hash, TO_BASE64(MD5(fc.fallback_caption_text))) AS caption_hash,
    
    COALESCE(pl.base_price, fc.fallback_price, 19.99) AS recommended_price,
    COALESCE(pl.price_ladder_json, '[17.99, 18.99, 19.99, 20.99, 21.99]') AS price_ladder_json,
    
    -- ADDED: Deterministic send time jitter (±10 minutes)
    DATETIME_ADD(
      DATETIME(bc.slot_dt_local, TIME(bc.hod_local, 0, 0)),
      INTERVAL MOD(ABS(FARM_FINGERPRINT(CONCAT(
        bc.username_page,
        FORMAT_DATE('%Y%m%d', bc.slot_dt_local),
        CAST(bc.hod_local AS STRING)
      ))), 21) - 10 MINUTE
    ) AS local_send_ts,
    
    COALESCE(bc.score_normalized, 0) AS score_final,
    bc.is_explorer AS explorer_flag,
    
    COALESCE(bc.cooldown_ok, FALSE) AS cooldown_ok,
    COALESCE(bc.quota_ok, TRUE) AS quota_ok,
    COALESCE(bc.dedupe_ok, FALSE) AS dedupe_ok,
    
    CASE
      WHEN NOT bc.is_compliant THEN 'compliance_filtered'
      WHEN bc.caption_id IS NULL THEN 'fallback_no_eligible'
      WHEN NOT bc.cooldown_ok THEN 'cooldown_override'
      WHEN NOT bc.dedupe_ok THEN 'dedup_override'
      ELSE bc.selection_reason
    END AS reason_code,
    
    bc.rps AS expected_rps,
    bc.conversion_rate AS expected_conversion,
    bc.dow_hod_percentile AS timing_quality,
    
    bc.category,
    bc.explicitness,
    bc.total_sent AS historical_sends,
    bc.days_since_used,
    bc.is_cold_start,
    
    CASE 
      WHEN bc.slot_rank = 0 THEN 'Prime slot - highest performance'
      WHEN bc.slot_rank = 1 THEN 'Secondary peak hour'
      ELSE 'Coverage slot'
    END AS slot_description,
    
    1024 AS bytes_billed_est,
    
    CURRENT_TIMESTAMP() AS generated_at
    
  FROM best_captions bc
  LEFT JOIN price_ladders pl 
    ON bc.username_page = pl.username_page
    AND bc.slot_dt_local = pl.slot_dt_local
    AND bc.hod_local = pl.hod_local
  LEFT JOIN fallback_config fc 
    ON bc.page_type = fc.page_type
    AND fc.reason_code = 'no_eligible_caption'
  LEFT JOIN scheduler_assignments sa
    ON bc.username_page = sa.username_page
  WHERE bc.final_rank = 1
),

-- Add price ladder rows (5 per slot as in original)
expanded_schedule AS (
  SELECT
    fs.*,
    pl.price AS slot_price,
    pl.ladder_position,
    CONCAT(
      fs.username_page, '__',
      FORMAT_DATE('%Y%m%d', fs.schedule_date), '__',
      CAST(fs.hod_local AS STRING), '__',
      CAST(pl.ladder_position AS STRING)
    ) AS row_key
  FROM final_schedule fs
  CROSS JOIN UNNEST(
    [
      STRUCT(1 AS ladder_position, fs.recommended_price - 2.0 AS price),
      STRUCT(2 AS ladder_position, fs.recommended_price - 1.0 AS price),
      STRUCT(3 AS ladder_position, fs.recommended_price AS price),  -- Canonical
      STRUCT(4 AS ladder_position, fs.recommended_price + 1.0 AS price),
      STRUCT(5 AS ladder_position, fs.recommended_price + 2.0 AS price)
    ]
  ) AS pl
)

-- Final output matching sheet columns exactly
SELECT
  schedule_date AS Date,
  FORMAT_DATE('%a', schedule_date) AS Day,
  username_std AS Model,
  page_type AS Page,
  CONCAT('Slot ', CAST(slot_rank + 1 AS STRING)) AS Slot,
  FORMAT_DATETIME('%H:%M', local_send_ts) AS `Rec Time`,  -- With jitter
  ROUND(slot_price, 2) AS `Rec Price`,
  
  caption_id AS `Rec Caption ID`,
  SUBSTR(caption_text, 1, 100) AS `Caption Preview`,
  
  CAST(NULL AS STRING) AS `Actual Time`,
  CAST(NULL AS FLOAT64) AS `Actual Price`,
  CAST(NULL AS STRING) AS `Caption ID`,
  
  'Planned' AS Status,
  slot_description AS Reason,
  ROUND(score_final, 0) AS Score,
  FALSE AS `Lock`,
  
  tracking_hash AS `Tracking Hash`,
  username_page AS `Username Page`,
  caption_hash AS `Caption Hash`,
  scheduler_name AS `Scheduler Name`,  -- For RLS
  ladder_position AS `Ladder Position`,  -- For QA filtering
  
  TO_JSON_STRING(STRUCT(
    caption_text AS full_text,
    category,
    explicitness,
    expected_rps,
    expected_conversion,
    timing_quality,
    historical_sends,
    days_since_used,
    is_cold_start,
    explorer_flag,
    cooldown_ok,
    quota_ok,
    dedupe_ok,
    reason_code,
    price_ladder_json,
    bytes_billed_est,
    local_send_ts AS send_ts_with_jitter
  )) AS metadata_json
  
FROM expanded_schedule
ORDER BY 
  schedule_date,
  username_std,
  page_type,
  slot_rank,
  ladder_position;