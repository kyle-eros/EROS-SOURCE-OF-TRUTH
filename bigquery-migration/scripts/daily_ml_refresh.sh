#!/bin/bash

# =========================================
# Daily ML Pipeline Refresh Script
# =========================================
# Purpose: Refresh all ML layers daily
# Schedule: Run at 2 AM UTC daily via cron
# =========================================

set -e  # Exit on error

PROJECT_ID="of-scheduler-proj"
SCRIPT_DIR="$(dirname "$0")"
LOG_DIR="$SCRIPT_DIR/../logs"
LOG_FILE="$LOG_DIR/ml_refresh_$(date +%Y%m%d_%H%M%S).log"

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

# Function to check table freshness
check_freshness() {
    local table=$1
    local date_column=$2
    
    result=$(bq query --use_legacy_sql=false --format=csv --project_id=$PROJECT_ID \
        "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX($date_column), HOUR) AS hours_old 
         FROM \`$PROJECT_ID.$table\`" 2>/dev/null | tail -n 1)
    
    echo "$result"
}

# =========================================
# MAIN EXECUTION
# =========================================

log_message "========================================="
log_message "Starting Daily ML Pipeline Refresh"
log_message "========================================="

# Step 1: Update Staging Layer (if needed)
log_message "Step 1: Checking Staging Layer freshness..."
staging_age=$(check_freshness "layer_02_staging.stg_message_events" "staging_timestamp")
if [ "$staging_age" -gt "24" ]; then
    run_query "$SCRIPT_DIR/../02_staging/01_stg_message_events.sql" "Refreshing Staging Layer"
else
    log_message "Staging Layer is fresh (${staging_age} hours old), skipping refresh"
fi

# Step 2: Update Semantic Layer - Caption Performance Daily
log_message "Step 2: Updating Semantic Layer..."
run_query "$SCRIPT_DIR/../04_semantic/01_caption_performance_daily.sql" "Updating Caption Performance Daily"

# Step 3: Update ML Feature Store (Incremental)
log_message "Step 3: Updating ML Feature Store..."
run_query "$SCRIPT_DIR/../05_ml/01_feature_store_production.sql" "Updating ML Feature Store (Incremental)"

# Step 4: Refresh Export Tables
log_message "Step 4: Refreshing Export Tables..."
run_query "$SCRIPT_DIR/../07_export/01_schedule_recommendations.sql" "Updating Schedule Recommendations"

# Step 5: Run Data Quality Checks
log_message "Step 5: Running Data Quality Checks..."
quality_check=$(bq query --use_legacy_sql=false --format=csv --project_id=$PROJECT_ID "
    SELECT 
        CASE
            WHEN COUNT(*) > 100 
             AND AVG(performance_features.confidence_score) > 0.05
             AND SUM(CASE WHEN cooldown_features.is_eligible THEN 1 ELSE 0 END) > 50
            THEN 'PASSED'
            ELSE 'FAILED'
        END AS quality_status
    FROM \`$PROJECT_ID.layer_05_ml.feature_store\`
    WHERE computed_date = CURRENT_DATE()
" 2>/dev/null | tail -n 1)

if [ "$quality_check" = "PASSED" ]; then
    log_message "✓ Data quality checks PASSED"
else
    log_message "⚠ Data quality checks FAILED - investigate immediately"
    # Could send alert here
fi

# Step 6: Log Summary Statistics
log_message "Step 6: Gathering Summary Statistics..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID >> "$LOG_FILE" 2>&1 <<EOF
SELECT
    'Summary' AS metric,
    (SELECT COUNT(*) FROM \`$PROJECT_ID.layer_05_ml.feature_store\` WHERE computed_date = CURRENT_DATE()) AS features_computed,
    (SELECT COUNT(*) FROM \`$PROJECT_ID.layer_07_export.schedule_recommendations\` WHERE schedule_date = CURRENT_DATE()) AS recommendations_generated,
    (SELECT AVG(performance_features.rps_smoothed) FROM \`$PROJECT_ID.layer_05_ml.feature_store\` WHERE computed_date = CURRENT_DATE()) AS avg_rps,
    CURRENT_TIMESTAMP() AS refresh_timestamp
EOF

# Step 7: Clean up old logs (keep last 30 days)
log_message "Step 7: Cleaning up old logs..."
find "$LOG_DIR" -name "*.log" -mtime +30 -delete

# Step 8: Export metrics for monitoring
log_message "Step 8: Exporting metrics for monitoring..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
INSERT INTO \`$PROJECT_ID.ops_monitor.pipeline_runs\`
SELECT
    CURRENT_TIMESTAMP() AS run_timestamp,
    'daily_ml_refresh' AS pipeline_name,
    '$quality_check' AS status,
    (SELECT COUNT(*) FROM \`$PROJECT_ID.layer_05_ml.feature_store\` WHERE computed_date = CURRENT_DATE()) AS records_processed,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP('$(date -u +%Y-%m-%d\ %H:%M:%S)'), SECOND) AS duration_seconds,
    'automated' AS trigger_type
EOF

log_message "========================================="
log_message "ML Pipeline Refresh Complete!"
log_message "Log saved to: $LOG_FILE"
log_message "========================================="

# Exit with appropriate code
if [ "$quality_check" = "PASSED" ]; then
    exit 0
else
    exit 1
fi