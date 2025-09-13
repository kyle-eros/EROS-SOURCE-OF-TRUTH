CREATE OR REPLACE VIEW `of-scheduler-proj.ops_config.page_state` AS
SELECT
  username_std AS username_page,                   -- canonical key
  IFNULL(is_active, TRUE) AS is_active,            -- boolean shape
  CASE WHEN IFNULL(is_active, TRUE) THEN "ACTIVE" ELSE "PAUSED" END AS state,  -- label shape
  username_std                                     -- legacy alias
FROM `of-scheduler-proj.layer_04_semantic.v_page_dim`;