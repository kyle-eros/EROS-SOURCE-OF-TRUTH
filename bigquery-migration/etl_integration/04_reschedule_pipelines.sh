#!/bin/bash

# =========================================
# RESCHEDULE ML PIPELINE TO RUN AFTER ETL
# =========================================
# Purpose: Update cron schedules for proper data flow
# Gmail ETL: 5:00 AM and 5:00 PM
# Transfer/Extract: 5:30 AM and 5:30 PM  
# ML Pipeline: 6:00 AM and 6:00 PM
# =========================================

set -e

echo "=========================================
UPDATING PIPELINE SCHEDULES
========================================="

# First, remove the old 2 AM cron job for ML pipeline
echo "Removing old ML pipeline schedule (2 AM)..."
crontab -l 2>/dev/null | grep -v "daily_ml_refresh.sh" | crontab - || true

# Get the current directory
SCRIPT_DIR="/Users/kylemerriman/Desktop/EROS-SYSTEM-SOURCE-OF-TRUTH/bigquery-migration"

# Ensure directories exist
mkdir -p "$SCRIPT_DIR/scripts" "$SCRIPT_DIR/logs" "$SCRIPT_DIR/etl_integration"

# Create the complete orchestration script
cat > "$SCRIPT_DIR/scripts/orchestrated_pipeline.sh" << 'EOF'
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
EOF

# Make the orchestration script executable
chmod +x "$SCRIPT_DIR/scripts/orchestrated_pipeline.sh"

# Create individual cron job script for 5:30 AM/PM runs
cat > "$SCRIPT_DIR/scripts/etl_integration_cron.sh" << 'EOF'
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
EOF

chmod +x "$SCRIPT_DIR/scripts/etl_integration_cron.sh"

# Update ML refresh script to run at 6 AM/PM
cat > "$SCRIPT_DIR/scripts/ml_pipeline_cron.sh" << 'EOF'
#!/bin/bash
# Run ML pipeline refresh
# This runs at 6:00 AM and 6:00 PM (1 hour after Gmail ETL)

/Users/kylemerriman/Desktop/EROS-SYSTEM-SOURCE-OF-TRUTH/bigquery-migration/scripts/daily_ml_refresh.sh
EOF

chmod +x "$SCRIPT_DIR/scripts/ml_pipeline_cron.sh"

# Now set up the new cron schedule
echo "Setting up new cron schedule..."

# Create the new crontab entries
(crontab -l 2>/dev/null || true; cat << CRON
# =========================================
# EROS ML SYSTEM - PIPELINE SCHEDULE
# =========================================
# Gmail ETL runs via Cloud Scheduler at 5:00 AM and 5:00 PM

# ETL Integration (Transfer, Extract, Map) - 5:30 AM and 5:30 PM
30 5,17 * * * $SCRIPT_DIR/scripts/etl_integration_cron.sh >> $SCRIPT_DIR/logs/etl_integration.log 2>&1

# ML Pipeline Refresh - 6:00 AM and 6:00 PM
0 6,18 * * * $SCRIPT_DIR/scripts/ml_pipeline_cron.sh >> $SCRIPT_DIR/logs/ml_pipeline.log 2>&1

# Optional: Full orchestration (commented out - use if you want single control)
# 0 5,17 * * * $SCRIPT_DIR/scripts/orchestrated_pipeline.sh >> $SCRIPT_DIR/logs/orchestrated.log 2>&1
CRON
) | crontab -

echo "✓ Cron jobs updated successfully!"
echo ""
echo "Current cron schedule:"
crontab -l | grep -E "(etl_integration|ml_pipeline|EROS)" || true

echo ""
echo "========================================="
echo "PIPELINE SCHEDULE UPDATED"
echo "========================================="
echo ""
echo "New Schedule:"
echo "  5:00 AM/PM - Gmail ETL (Cloud Scheduler)"
echo "  5:30 AM/PM - ETL Integration (Transfer/Extract/Map)"
echo "  6:00 AM/PM - ML Pipeline Refresh"
echo ""
echo "Data Flow:"
echo "  Gmail → staging.gmail_etl_daily"
echo "       ↓ (5:30)"
echo "  Transfer → fact_message_send"
echo "  Extract → caption_library"
echo "  Map → username_mapping"
echo "       ↓ (6:00)"
echo "  ML Refresh → feature_store → rankings"
echo ""
echo "Commands:"
echo "  View logs: tail -f $SCRIPT_DIR/logs/*.log"
echo "  Test integration: $SCRIPT_DIR/scripts/etl_integration_cron.sh"
echo "  Test ML refresh: $SCRIPT_DIR/scripts/ml_pipeline_cron.sh"
echo "  Run full pipeline: $SCRIPT_DIR/scripts/orchestrated_pipeline.sh --no-wait"