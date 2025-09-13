#!/bin/bash
# Run ETL integration steps (transfer, extract, map)
# This runs at 5:30 AM and 5:30 PM

set -e
PROJECT_ID="of-scheduler-proj"
SCRIPT_DIR="$(dirname "$0")"

# Run the three integration queries
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < "$SCRIPT_DIR/../etl_integration/01_transfer_staging_to_fact.sql"
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < "$SCRIPT_DIR/../etl_integration/02_extract_new_captions.sql"
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < "$SCRIPT_DIR/../etl_integration/03_update_username_mapping.sql"
