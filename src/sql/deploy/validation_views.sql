-- File: $OUT_DIR/validation_views.sql
-- Quality validation and monitoring views

-- Quality check view
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_caption_bank_quality_v1` AS
SELECT
  DATE(created_at) as insert_date,
  COUNT(*) as captions_added,
  COUNT(DISTINCT caption_hash_v2) as unique_captions,
  COUNT(DISTINCT last_used_page) as unique_pages,
  STRING_AGG(DISTINCT explicitness) as explicitness_values,
  STRING_AGG(DISTINCT length_cat) as length_categories,
  AVG(`of-scheduler-proj.util.emoji_count`(caption_text)) as avg_emoji_count,
  COUNTIF(`of-scheduler-proj.util.has_cta`(caption_text)) as captions_with_cta,
  COUNTIF(`of-scheduler-proj.util.has_urgency`(caption_text)) as captions_with_urgency,
  COUNTIF(`of-scheduler-proj.util.ends_with_question`(caption_text)) as question_captions,
  -- Additional quality metrics
  AVG(LENGTH(caption_text)) as avg_text_length,
  COUNTIF(LENGTH(TRIM(caption_text)) < 10) as very_short_captions,
  COUNTIF(LENGTH(TRIM(caption_text)) > 500) as very_long_captions,
  COUNTIF(theme_tags = 'untagged') as untagged_captions
FROM `of-scheduler-proj.raw.caption_library`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY 1
ORDER BY 1 DESC;

-- Ingestion monitoring view
CREATE OR REPLACE VIEW `of-scheduler-proj.ops.v_caption_ingestion_monitor` AS
SELECT
  DATE(run_ts) as run_date,
  COUNT(*) as runs,
  SUM(new_captions_inserted) as total_inserted,
  AVG(new_captions_inserted) as avg_inserted,
  MAX(new_captions_inserted) as max_inserted,
  SUM(duplicates_detected) as total_duplicates,
  SUM(error_count) as total_errors,
  STRING_AGG(error_sample LIMIT 3) as recent_errors,
  -- Performance metrics
  AVG(source_rows) as avg_source_rows,
  SAFE_DIVIDE(SUM(new_captions_inserted), SUM(source_rows)) as insertion_rate,
  SAFE_DIVIDE(SUM(duplicates_detected), SUM(source_rows)) as duplicate_rate
FROM `of-scheduler-proj.ops.caption_ingestion_log_v1`
WHERE run_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY 1 DESC;