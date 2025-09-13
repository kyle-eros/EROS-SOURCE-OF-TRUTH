# ===============================
# Finalize Gmail ETL migration
# ===============================

set -euo pipefail

# ---- Context ----
export PROJECT_ID="of-scheduler-proj"
export LOCATION="US"
export LEGACY_DATASET="staging"
export STAGING_DATASET="layer_02_staging"
export FOUNDATION_DATASET="layer_03_foundation"

# (Optional) avoid surprises
gcloud config set project "${PROJECT_ID}"

# ---- Auto-expire legacy tables ----
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
ALTER TABLE \`${PROJECT_ID}.${LEGACY_DATASET}.gmail_etl_daily\`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY));
"

bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
ALTER TABLE \`${PROJECT_ID}.${LEGACY_DATASET}.gmail_etl_daily_legacy_20250911\`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY));
"

# ---- Mark legacy as deprecated (UI hint) ----
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
ALTER TABLE \`${PROJECT_ID}.${LEGACY_DATASET}.gmail_etl_daily\`
SET OPTIONS (description = 'DEPRECATED — replaced by ${STAGING_DATASET}.gmail_events_staging');
"

# ---- Nightly fact refresh (1-day ingestion window) ----
cat > nightly_fact_upsert.sql <<'SQL'
CALL `of-scheduler-proj.layer_03_foundation.sp_upsert_fact_gmail_message_send`(
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
  CURRENT_DATE()
);
SQL
echo "Created nightly_fact_upsert.sql (schedule this as a BigQuery Scheduled Query: pipeline=gmail_etl, stage=foundation)."

# ---- (Optional) Cost canary (requires region-us JOBS access) ----
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
SELECT DATE(creation_time) AS day,
       SUM(total_bytes_processed) AS bytes_processed
FROM  \`region-us\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE project_id='${PROJECT_ID}'
  AND job_type='QUERY'
  AND labels['pipeline']='gmail_etl'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY 1
ORDER BY 1 DESC;
" || echo '[info] Skipping cost canary (no region-us access).'

# ---- Data-quality canary (should be near-zero) ----
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
SELECT
  COUNTIF(message_id IS NULL)      AS id_nulls,
  COUNTIF(source_file IS NULL)     AS src_nulls,
  COUNTIF(message_sent_ts IS NULL) AS ts_nulls
FROM \`${PROJECT_ID}.${STAGING_DATASET}.fn_gmail_events_normalized\`(
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), CURRENT_DATE()
);
"

# ---- Canonicalize fact naming (optional compat view) ----
HAS_OLD_FACT="$(bq --project_id="${PROJECT_ID}" ls "${FOUNDATION_DATASET}" | awk '{print $1}' | grep -qx 'fact_message_send' && echo 1 || echo 0)"
if [[ "${HAS_OLD_FACT}" == "1" ]]; then
  bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
  CREATE OR REPLACE VIEW \`${PROJECT_ID}.${FOUNDATION_DATASET}.fact_message_send\` AS
  SELECT * FROM \`${PROJECT_ID}.${FOUNDATION_DATASET}.fact_gmail_message_send\`;
  "
  echo "[info] Created compat view ${FOUNDATION_DATASET}.fact_message_send -> fact_gmail_message_send"
fi

# ---- (Optional but smart) enforce partition safety on quarantine ----
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
ALTER TABLE \`${PROJECT_ID}.ops.quarantine_gmail\`
SET OPTIONS (require_partition_filter = TRUE);
" || true

# ---- Quick smoke checks ----
bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
SELECT COUNT(*) AS rows_today
FROM \`${PROJECT_ID}.${STAGING_DATASET}.gmail_events_staging\`
WHERE ingestion_date = CURRENT_DATE();
"

bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
SELECT COUNT(*) AS rows_7d
FROM \`${PROJECT_ID}.${STAGING_DATASET}.fn_gmail_events_normalized\`(
  DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), CURRENT_DATE()
);
"

bq --project_id="${PROJECT_ID}" --location="${LOCATION}" query --use_legacy_sql=false "
SELECT COUNT(*) AS fact_rows_7d
FROM \`${PROJECT_ID}.${FOUNDATION_DATASET}.fact_gmail_message_send\`
WHERE message_sent_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE();
"