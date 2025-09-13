CREATE OR REPLACE VIEW `of-scheduler-proj.mart.v_send_to_perf_link_180d` AS
WITH ss AS (
  SELECT * FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page`
),
mm AS (
  SELECT username_std, sending_ts, caption_hash, price_usd, earnings_usd, sent, viewed, purchased, sender
  FROM `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
),
cl AS (
  SELECT username_std, caption_id, caption_hash
  FROM `of-scheduler-proj.layer_04_semantic.v_caption_dim`
),
cand AS (
  SELECT
    ss.username_std, ss.logged_ts, ss.scheduler_name, ss.caption_id, ss.was_modified,
    ss.price_usd_scheduled, ss.tracking_hash,
    mm.sending_ts, mm.caption_hash AS hash_msg,
    mm.price_usd, mm.earnings_usd, mm.sent, mm.viewed, mm.purchased, mm.sender,
    CASE
      WHEN ss.tracking_hash IS NOT NULL AND ss.tracking_hash = mm.caption_hash THEN 3
      WHEN ss.caption_id IS NOT NULL AND EXISTS (
        SELECT 1 FROM cl
        WHERE cl.username_std = ss.username_std
          AND cl.caption_id   = ss.caption_id
          AND cl.caption_hash = mm.caption_hash
      ) THEN 2
      WHEN ss.price_usd_scheduled IS NOT NULL
           AND ABS(ss.price_usd_scheduled - mm.price_usd) < 0.01
           AND mm.sending_ts BETWEEN TIMESTAMP_SUB(ss.logged_ts, INTERVAL 1 DAY)
                                 AND TIMESTAMP_ADD(ss.logged_ts, INTERVAL 14 DAY) THEN 1
      ELSE 0
    END AS match_score,
    ABS(TIMESTAMP_DIFF(mm.sending_ts, ss.logged_ts, MINUTE)) AS dt_min
  FROM ss
  JOIN mm USING (username_std)
  WHERE (ss.tracking_hash IS NOT NULL AND ss.tracking_hash = mm.caption_hash)
     OR (ss.caption_id IS NOT NULL AND EXISTS (
           SELECT 1 FROM cl
           WHERE cl.username_std = ss.username_std
             AND cl.caption_id   = ss.caption_id
             AND cl.caption_hash = mm.caption_hash))
     OR (ss.price_usd_scheduled IS NOT NULL
         AND ABS(ss.price_usd_scheduled - mm.price_usd) < 0.01
         AND mm.sending_ts BETWEEN TIMESTAMP_SUB(ss.logged_ts, INTERVAL 1 DAY)
                               AND TIMESTAMP_ADD(ss.logged_ts, INTERVAL 14 DAY))
)
SELECT
  username_std,
  DATE(sending_ts) AS sent_date,
  scheduler_name,
  sender,
  logged_ts,
  caption_id,
  was_modified,
  price_usd_scheduled,
  sending_ts,
  hash_msg,
  price_usd,
  earnings_usd,
  sent,
  viewed,
  purchased,
  CASE match_score WHEN 3 THEN 'hash' WHEN 2 THEN 'caption_id' ELSE 'time_price' END AS matched_by
FROM cand
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY username_std, logged_ts, caption_id
  ORDER BY match_score DESC, dt_min ASC
) = 1
