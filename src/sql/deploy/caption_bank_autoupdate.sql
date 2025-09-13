-- File: $OUT_DIR/caption_bank_autoupdate.sql
-- MERGE operation with comprehensive error handling

DECLARE run_id STRING DEFAULT GENERATE_UUID();
DECLARE window_days INT64 DEFAULT 30;
DECLARE source_count INT64 DEFAULT 0;
DECLARE insert_count INT64 DEFAULT 0;
DECLARE error_msg STRING DEFAULT NULL;

BEGIN
  -- Count source rows
  SET source_count = (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_new_captions_inflow_v1`);
  
  -- Insert new captions into raw.caption_library
  MERGE `of-scheduler-proj.raw.caption_library` T
  USING (
    SELECT
      GENERATE_UUID() AS caption_id,
      COALESCE(c.caption_hash, `of-scheduler-proj.util.caption_hash_v2`(c.caption_text)) AS caption_hash,
      `of-scheduler-proj.util.caption_hash_v2`(c.caption_text) AS caption_hash_v2,
      c.caption_text,
      'main' AS caption_type,
      `of-scheduler-proj.util.detect_explicitness`(c.caption_text) AS explicitness,
      COALESCE(NULLIF(`of-scheduler-proj.util.compute_theme_tags`(c.caption_text), ''), 'untagged') AS theme_tags,
      `of-scheduler-proj.util.length_bin`(c.caption_text) AS length_cat,
      CAST(NULL AS NUMERIC) AS price_last_sent,
      CAST(NULL AS STRING) AS last_used_by,
      CAST(NULL AS TIMESTAMP) AS last_used_date,
      c.username_page AS last_used_page,
      CAST(0 AS INT64) AS times_used,
      CURRENT_TIMESTAMP() AS created_at,
      CURRENT_TIMESTAMP() AS updated_at
    FROM `of-scheduler-proj.core.v_new_captions_inflow_v1` c
  ) S
  ON FALSE  -- Insert-only (no updates in v1)
  WHEN NOT MATCHED THEN INSERT (
    caption_id, caption_hash, caption_hash_v2, caption_text, caption_type,
    explicitness, theme_tags, length_cat,
    price_last_sent, last_used_by, last_used_date, last_used_page, times_used,
    created_at, updated_at
  )
  VALUES (
    S.caption_id, S.caption_hash, S.caption_hash_v2, S.caption_text, S.caption_type,
    S.explicitness, S.theme_tags, S.length_cat,
    S.price_last_sent, S.last_used_by, S.last_used_date, S.last_used_page, S.times_used,
    S.created_at, S.updated_at
  );
  
  -- Count actual insertions (approximation based on recent timestamp)
  SET insert_count = (
    SELECT COUNT(*) 
    FROM `of-scheduler-proj.raw.caption_library`
    WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
  );
  
  -- Log successful run
  INSERT INTO `of-scheduler-proj.ops.caption_ingestion_log_v1`
  VALUES (
    run_id,
    CURRENT_TIMESTAMP(),
    source_count,
    insert_count,
    0,  -- missing_text
    source_count - insert_count,  -- duplicates_detected
    window_days,
    0,  -- error_count
    NULL  -- error_sample
  );

EXCEPTION WHEN ERROR THEN
  -- Log error
  SET error_msg = @@error.message;
  
  INSERT INTO `of-scheduler-proj.ops.caption_ingestion_log_v1`
  VALUES (
    run_id,
    CURRENT_TIMESTAMP(),
    COALESCE(source_count, 0),
    0,  -- new_captions_inserted
    0,  -- missing_text
    0,  -- duplicates_detected  
    window_days,
    1,  -- error_count
    error_msg
  );
  
  -- Re-raise the error
  RAISE USING MESSAGE = error_msg;
END;