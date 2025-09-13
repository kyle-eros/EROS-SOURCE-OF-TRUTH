-- Create migration.v_caption_safe_candidates
CREATE OR REPLACE VIEW `of-scheduler-proj.migration.v_caption_safe_candidates` AS
SELECT
  username_page,
  caption_id,
  caption_hash,
  caption_text,
  len_bin,
  emoji_bin,
  has_cta,
  has_urgency,
  ends_with_question
FROM `of-scheduler-proj.layer_04_semantic.v_caption_candidates`;

-- Create migration.v_caption_last_used
CREATE OR REPLACE VIEW `of-scheduler-proj.migration.v_caption_last_used` AS
SELECT
  username_page,
  caption_hash,
  MAX(sent_at) AS last_used_ts
FROM `of-scheduler-proj.layer_04_semantic.v_caption_send_history`
GROUP BY username_page, caption_hash;