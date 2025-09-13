CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_username_resolver` AS
SELECT
  username_std AS username_page,
  username_std
FROM `of-scheduler-proj.layer_04_semantic.v_username_canonical`;