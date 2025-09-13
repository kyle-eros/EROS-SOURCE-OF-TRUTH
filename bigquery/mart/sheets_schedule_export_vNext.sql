-- =====================================================
-- SHEETS SCHEDULE EXPORT WITH COMPLETE CONTRACT v.Next
-- =====================================================
-- Project: of-scheduler-proj
-- Purpose: Final view for Google Sheets with all required columns including caption_id and caption_text
-- Contract: THIS IS THE CRITICAL FIX - provides caption_id, caption_text, and price ladders
-- Partition: DATE(schedule_date)
-- Cluster: username_page, schedule_date
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.mart.sheets_schedule_export_vNext` AS
WITH 
-- Get price bands configuration
price_config AS (
  SELECT
    pb.*,
    COALESCE(pt.tier_final, 'standard') AS tier
  FROM `of-scheduler-proj.ops.price_bands_v1` pb
  CROSS JOIN `of-scheduler-proj.core.v_page_tier_final_v1` pt
  WHERE pb.updated_at = (
    SELECT MAX(updated_at) 
    FROM `of-scheduler-proj.ops.price_bands_v1`
  )
),

-- Get fallback configuration
fallback_config AS (
  SELECT *
  FROM `of-scheduler-proj.ops.fallback_config_v1`
  WHERE updated_at = (
    SELECT MAX(updated_at)
    FROM `of-scheduler-proj.ops.fallback_config_v1`
  )
),

-- Get top ranked caption per slot
best_captions AS (
  SELECT
    cr.*,
    ROW_NUMBER() OVER (
      PARTITION BY username_page, slot_dt_local, hod_local
      ORDER BY 
        -- Prioritize compliant captions
        CASE WHEN cooldown_ok AND quota_ok AND dedupe_ok THEN 0 ELSE 1 END,
        score_final DESC
    ) AS final_rank
  FROM `of-scheduler-proj.mart.caption_ranker_vNext` cr
  WHERE rank_in_slot = 1  -- Already the best per slot from ranker
),

-- Generate price ladders
price_ladders AS (
  SELECT
    bc.username_page,
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
        3 AS ladder_position
      )
    ] AS price_ladder_array,
    -- JSON format for sheets
    TO_JSON_STRING([
      ROUND(pc.optimal_price - pc.ladder_increment, 2),
      ROUND(pc.optimal_price, 2),
      ROUND(pc.optimal_price + pc.ladder_increment, 2)
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
    SPLIT(bc.username_page, '__')[SAFE_OFFSET(0)] AS username_std,
    bc.page_type,
    bc.slot_dt_local AS schedule_date,
    bc.hod_local,
    bc.dow_local,
    bc.slot_rank,
    bc.tracking_hash,
    
    -- CRITICAL FIX: Provide both caption_id and caption_text
    COALESCE(
      CASE 
        WHEN bc.cooldown_ok AND bc.quota_ok AND bc.dedupe_ok THEN bc.caption_id
        ELSE NULL
      END,
      fc.fallback_caption_id
    ) AS caption_id,
    
    COALESCE(
      CASE 
        WHEN bc.cooldown_ok AND bc.quota_ok AND bc.dedupe_ok THEN bc.caption_text
        ELSE NULL
      END,
      fc.fallback_caption_text
    ) AS caption_text,
    
    -- Caption hash for backwards compatibility
    COALESCE(bc.caption_hash, TO_BASE64(MD5(fc.fallback_caption_text))) AS caption_hash,
    
    -- Price and ladder
    COALESCE(pl.base_price, fc.fallback_price, 19.99) AS recommended_price,
    COALESCE(pl.price_ladder_json, '[17.99, 19.99, 21.99]') AS price_ladder_json,
    
    -- Local send time
    DATETIME(bc.slot_dt_local, TIME(bc.hod_local, 0, 0)) AS local_send_ts,
    
    -- Scores and flags
    COALESCE(bc.score_normalized, 0) AS score_final,
    bc.is_explorer AS explorer_flag,
    
    -- Compliance flags
    COALESCE(bc.cooldown_ok, FALSE) AS cooldown_ok,
    COALESCE(bc.quota_ok, TRUE) AS quota_ok,
    COALESCE(bc.dedupe_ok, FALSE) AS dedupe_ok,
    
    -- Reason codes
    CASE
      WHEN bc.caption_id IS NULL THEN 'fallback_no_eligible'
      WHEN NOT bc.cooldown_ok THEN 'cooldown_override'
      WHEN NOT bc.dedupe_ok THEN 'dedup_override'
      ELSE bc.selection_reason
    END AS reason_code,
    
    -- Performance hints
    bc.rps AS expected_rps,
    bc.conversion_rate AS expected_conversion,
    bc.dow_hod_percentile AS timing_quality,
    
    -- Metadata
    bc.category,
    bc.explicitness,
    bc.total_sent AS historical_sends,
    bc.days_since_used,
    bc.is_cold_start,
    
    -- Slot description for UI
    CASE 
      WHEN bc.slot_rank = 0 THEN 'Prime slot - highest performance'
      WHEN bc.slot_rank = 1 THEN 'Secondary peak hour'
      ELSE 'Coverage slot'
    END AS slot_description,
    
    -- Estimated bytes for cost tracking
    1024 AS bytes_billed_est,
    
    -- Audit
    CURRENT_TIMESTAMP() AS generated_at
    
  FROM best_captions bc
  LEFT JOIN price_ladders pl 
    ON bc.username_page = pl.username_page
    AND bc.slot_dt_local = pl.slot_dt_local
    AND bc.hod_local = pl.hod_local
  LEFT JOIN fallback_config fc 
    ON bc.page_type = fc.page_type
  WHERE bc.final_rank = 1
),

-- Add price ladder rows (5 per slot as in original CSV)
expanded_schedule AS (
  SELECT
    fs.*,
    pl.price AS slot_price,
    pl.ladder_position,
    -- Unique row key for sheets
    CONCAT(
      fs.username_page, '__',
      FORMAT_DATE('%Y%m%d', fs.schedule_date), '__',
      CAST(fs.hod_local AS STRING), '__',
      CAST(pl.ladder_position AS STRING)
    ) AS row_key
  FROM final_schedule fs
  CROSS JOIN UNNEST(
    -- Generate ladder positions
    [
      STRUCT(1 AS ladder_position, fs.recommended_price - 2.0 AS price),
      STRUCT(2 AS ladder_position, fs.recommended_price - 1.0 AS price),
      STRUCT(3 AS ladder_position, fs.recommended_price AS price),
      STRUCT(4 AS ladder_position, fs.recommended_price + 1.0 AS price),
      STRUCT(5 AS ladder_position, fs.recommended_price + 2.0 AS price)
    ]
  ) AS pl
)

-- Final output matching sheet columns exactly
SELECT
  -- Core scheduling columns
  schedule_date AS Date,
  FORMAT_DATE('%a', schedule_date) AS Day,
  username_std AS Model,
  page_type AS Page,
  CONCAT('Slot ', CAST(slot_rank + 1 AS STRING)) AS Slot,
  FORMAT('%d:00', hod_local) AS `Rec Time`,
  ROUND(slot_price, 2) AS `Rec Price`,
  
  -- CRITICAL: Caption ID and Preview
  caption_id AS `Rec Caption ID`,
  SUBSTR(caption_text, 1, 100) AS `Caption Preview`,
  
  -- Actual columns (for manual overrides)
  CAST(NULL AS STRING) AS `Actual Time`,
  CAST(NULL AS FLOAT64) AS `Actual Price`,
  CAST(NULL AS STRING) AS `Caption ID`,
  
  -- Status and metadata
  'Planned' AS Status,
  slot_description AS Reason,
  ROUND(score_final, 0) AS Score,
  FALSE AS `Lock`,
  
  -- Hidden columns for Apps Script
  tracking_hash AS `Tracking Hash`,
  username_page AS `Username Page`,
  caption_hash AS `Caption Hash`,
  
  -- Additional metadata for picker
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
    bytes_billed_est
  )) AS metadata_json
  
FROM expanded_schedule
ORDER BY 
  schedule_date,
  username_std,
  page_type,
  slot_rank,
  ladder_position;