CREATE OR REPLACE VIEW `of-scheduler-proj.layer_05_ml.v_caption_features`
OPTIONS(description="Refactored view to generate caption features from the new foundation layer, serving as a clean input for the ML feature store.")
AS
SELECT
  d.caption_key,
  d.caption_id,
  d.caption_hash,
  d.caption_text,
  d.caption_category,
  d.content_tags,
  d.explicitness_level,
  d.creator_username,
  d.username_page,
  -- Basic text features
  d.caption_length AS len_words,
  `of-scheduler-proj.util.emoji_count`(d.caption_text) AS emoji_cnt,
  `of-scheduler-proj.util.length_bin`(d.caption_text) AS len_bin,
  `of-scheduler-proj.util.emoji_bin`(d.caption_text) AS emoji_bin,
  -- Semantic features
  `of-scheduler-proj.util.has_cta`(d.caption_text) AS has_cta,
  `of-scheduler-proj.util.has_urgency`(d.caption_text) AS has_urgency,
  `of-scheduler-proj.util.ends_with_question`(d.caption_text) AS ends_with_question
FROM `of-scheduler-proj.layer_03_foundation.dim_caption` d
WHERE d.is_active = TRUE;