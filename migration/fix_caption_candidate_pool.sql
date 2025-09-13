CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_caption_candidate_pool_v3` AS
WITH cd AS (
  SELECT CAST(setting_val AS INT64) AS cooldown_days
  FROM `of-scheduler-proj.ops_config.settings_modeling`
  WHERE setting_key="caption_cooldown_days"
)
SELECT
  f.username_page, f.caption_id, f.caption_hash, f.caption_text,
  f.len_bin, f.emoji_bin, f.has_cta, f.has_urgency, f.ends_with_question,
  lu.last_used_ts,
  CASE
    WHEN lu.last_used_ts IS NULL THEN TRUE
    WHEN lu.last_used_ts < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (SELECT cooldown_days FROM cd LIMIT 1) DAY) THEN TRUE
    ELSE FALSE
  END AS is_cooldown_ok
FROM `of-scheduler-proj.layer_05_ml.v_caption_safe_candidates` f
LEFT JOIN `of-scheduler-proj.layer_05_ml.v_caption_last_used` lu
  ON f.creator_username = lu.username_std AND f.caption_hash = lu.caption_hash;