CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_message_facts_by_page`
OPTIONS(description="All message facts joined with creator page context (default 365d window for partition filter).") AS
SELECT
  f.*,
  CONCAT(c.username, '__', COALESCE(c.account_type,'main')) AS username_page
FROM `of-scheduler-proj.layer_03_foundation.fact_message_send` f
JOIN `of-scheduler-proj.layer_03_foundation.dim_creator`     c
  ON f.creator_key = c.creator_key AND c.is_current_record = TRUE
WHERE f.send_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 365 DAY);   -- partition guard