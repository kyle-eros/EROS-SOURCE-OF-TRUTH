WITH m AS (
  SELECT username_std, sending_ts, earnings_usd
  FROM `of-scheduler-proj.layer_04_semantic.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
),
assign AS (
  SELECT username_std, assigned_scheduler
  FROM `of-scheduler-proj.layer_04_semantic.v_page_dim`
)
SELECT
  assign.assigned_scheduler AS scheduler,
  SUM(IF(m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY), m.earnings_usd, 0)) AS rev_7d,
  SUM(m.earnings_usd) AS rev_28d,
  COUNTIF(m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS msgs_7d,
  COUNT(*) AS msgs_28d
FROM m
JOIN assign USING (username_std)
GROUP BY scheduler
ORDER BY rev_28d DESC
