SELECT 'Pages Count' AS metric, COUNT(*) AS value FROM `of-scheduler-proj.layer_04_semantic.v_pages`
UNION ALL
SELECT 'Message Facts 365d', COUNT(*) FROM `of-scheduler-proj.mart.message_facts_by_page_365d`
UNION ALL
SELECT 'Page Health 7d', COUNT(*) FROM `of-scheduler-proj.layer_04_semantic.v_page_health_7d`
UNION ALL
SELECT 'Fresh Tier Thresholds', COUNT(*) FROM `of-scheduler-proj.ops_config.tier_thresholds`
WHERE computed_date >= DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 7 DAY);