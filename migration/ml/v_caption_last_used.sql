CREATE OR REPLACE VIEW `of-scheduler-proj.layer_05_ml.v_caption_last_used`
OPTIONS(description="Last time a caption was used by a creator (stable on creator_key + caption_hash).") AS
SELECT
  f.creator_key,                           -- stable page id
  c.username AS username_std,              -- human-readable
  d.caption_hash,
  MAX(f.send_timestamp) AS last_used_ts
FROM `of-scheduler-proj.layer_03_foundation.fact_message_send` AS f
JOIN `of-scheduler-proj.layer_03_foundation.dim_creator`  AS c
  ON f.creator_key = c.creator_key AND c.is_current_record = TRUE
JOIN `of-scheduler-proj.layer_03_foundation.dim_caption`  AS d
  ON f.caption_key = d.caption_key
WHERE COALESCE(f.quality_flag,'valid') = 'valid'
  AND f.send_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 365 DAY)  -- partition guard
GROUP BY 1,2,3;