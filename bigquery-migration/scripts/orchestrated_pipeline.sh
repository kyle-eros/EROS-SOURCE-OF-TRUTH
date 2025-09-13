#!/bin/bash

# =========================================
# ORCHESTRATED DATA PIPELINE
# =========================================
# Runs the complete data flow in order:
# 1. Gmail ETL (already runs at 5 AM/PM via Cloud Scheduler)
# 2. Transfer & Extract (5:30 AM/PM)
# 3. ML Pipeline Refresh (6:00 AM/PM)
# =========================================

set -e

PROJECT_ID="of-scheduler-proj"
SCRIPT_DIR="$(dirname "$0")"
LOG_DIR="$SCRIPT_DIR/../logs"
LOG_FILE="$LOG_DIR/orchestrated_pipeline_$(date +%Y%m%d_%H%M%S).log"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to run BigQuery query
run_query() {
    local sql_file=$1
    local description=$2
    
    log_message "Running: $description"
    
    if bq query --use_legacy_sql=false --project_id=$PROJECT_ID < "$sql_file" >> "$LOG_FILE" 2>&1; then
        log_message "✓ Success: $description"
        return 0
    else
        log_message "✗ Failed: $description"
        return 1
    fi
}

log_message "========================================="
log_message "Starting Orchestrated Pipeline"
log_message "========================================="

# Check what time it is to determine which run this is
HOUR=$(date +%H)
if [ "$HOUR" -lt "12" ]; then
    RUN_TYPE="morning"
else
    RUN_TYPE="evening"
fi
log_message "Run type: $RUN_TYPE (Hour: $HOUR)"

# Wait a bit to ensure Gmail ETL has completed (it starts at 5:00)
if [ "$1" != "--no-wait" ]; then
    log_message "Waiting 30 minutes for Gmail ETL to complete..."
    sleep 1800  # 30 minutes
fi

# Step 1: Transfer staging data to fact table
log_message "Step 1: Transferring staging data to fact table..."
run_query "$SCRIPT_DIR/../etl_integration/01_transfer_staging_to_fact.sql" "Transfer staging to fact_message_send"

# Step 2: Extract new captions
log_message "Step 2: Extracting new captions..."
run_query "$SCRIPT_DIR/../etl_integration/02_extract_new_captions.sql" "Extract new captions to library"

# Step 3: Update username mappings
log_message "Step 3: Updating username mappings..."
run_query "$SCRIPT_DIR/../etl_integration/03_update_username_mapping.sql" "Update username mappings"

# Step 4: Wait before running ML pipeline
log_message "Waiting 30 minutes before ML pipeline..."
sleep 1800  # 30 more minutes (total 1 hour after ETL start)

# Step 5: Run ML pipeline refresh
log_message "Step 5: Running ML pipeline refresh..."
$SCRIPT_DIR/daily_ml_refresh.sh

log_message "========================================="
log_message "Orchestrated Pipeline Complete!"
log_message "========================================="
