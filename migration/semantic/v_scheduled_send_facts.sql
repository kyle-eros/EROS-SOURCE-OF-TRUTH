CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_scheduled_send_facts`
OPTIONS(description="Refactored view for scheduled send facts. NOTE: This requires a new source table in the staging layer, as the legacy `raw.scheduled_sends` table will be deprecated.")
AS
-- This view cannot be fully refactored without a new source table for scheduled sends.
-- The query below is a placeholder structure.
SELECT
  -- `of-scheduler-proj.util.norm_username`(model_name) AS username_std,
  -- LOWER(NULLIF(scheduler_name,'')) AS scheduler_name,
  -- TIMESTAMP_TRUNC(CAST(logged_ts AS TIMESTAMP), MINUTE) AS logged_ts,
  -- CAST(price_usd AS NUMERIC) AS price_usd_scheduled,
  -- COALESCE(NULLIF(tracking_hash_v2,''), NULLIF(tracking_hash,'')) AS tracking_hash,
  -- CAST(caption_id AS STRING) AS caption_id,
  -- CAST(was_modified AS BOOL) AS was_modified
  CAST(NULL AS STRING) AS username_std,
  CAST(NULL AS STRING) AS scheduler_name,
  CAST(NULL AS TIMESTAMP) AS logged_ts,
  CAST(NULL AS NUMERIC) AS price_usd_scheduled,
  CAST(NULL AS STRING) AS tracking_hash,
  CAST(NULL AS STRING) AS caption_id,
  CAST(NULL AS BOOL) AS was_modified
FROM `of-scheduler-proj.layer_03_foundation.dim_creator` -- Placeholder FROM
LIMIT 0;