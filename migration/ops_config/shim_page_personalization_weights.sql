CREATE OR REPLACE VIEW `of-scheduler-proj.ops_config.page_personalization_weights` AS
SELECT
  username_std AS username_page,
  1.0 AS weight_recency,
  1.0 AS weight_engagement,
  1.0 AS weight_revenue
FROM `of-scheduler-proj.layer_04_semantic.v_pages`;