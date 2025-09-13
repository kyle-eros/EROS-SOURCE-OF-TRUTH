CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page_90d`
OPTIONS(description="Refactored view to provide message facts from the last 90 days, joined with creator page context.")
AS
SELECT
  f.*,
  CONCAT(c.username, '__', c.account_type) AS username_page
FROM `of-scheduler-proj.layer_03_foundation.fact_message_send` f
JOIN `of-scheduler-proj.layer_03_foundation.dim_creator` c
  ON f.creator_key = c.creator_key AND c.is_current_record = TRUE
WHERE f.send_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 89 DAY);