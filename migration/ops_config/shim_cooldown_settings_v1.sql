CREATE OR REPLACE VIEW `of-scheduler-proj.ops_config.cooldown_settings_v1` AS
SELECT
  "caption" AS entity,
  CAST(setting_val AS INT64) AS cooldown_days,     -- numeric (most callers use this)
  "caption_cooldown_days" AS setting_key,          -- legacy key shape (string)
  CAST(setting_val AS STRING) AS setting_value     -- legacy value shape (string)
FROM `of-scheduler-proj.ops_config.settings_modeling`
WHERE setting_key = "caption_cooldown_days";