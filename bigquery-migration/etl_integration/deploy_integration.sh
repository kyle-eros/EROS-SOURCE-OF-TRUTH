#!/bin/bash

# =========================================
# DEPLOY ETL INTEGRATION WITH ML PIPELINE
# =========================================
# Purpose: Deploy all integration components
# 1. Create scheduled queries for data transfer
# 2. Update cron schedules for ML pipeline
# 3. Verify system configuration
# =========================================

set -e

PROJECT_ID="of-scheduler-proj"
REGION="us-central1"
SCRIPT_DIR="$(dirname "$0")"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

echo "========================================="
echo "DEPLOYING ETL → ML INTEGRATION"
echo "========================================="
echo ""

# Step 1: Create BigQuery scheduled queries
log_info "Creating BigQuery scheduled queries..."

# Create transfer query (5:30 AM and 5:30 PM Denver time)
log_info "Creating transfer query schedule..."
bq mk --transfer_config \
    --project_id="$PROJECT_ID" \
    --location="$REGION" \
    --display_name="ETL Transfer: Staging to Fact (5:30 AM/PM)" \
    --data_source=scheduled_query \
    --target_dataset=layer_03_foundation \
    --schedule="every day 05:30,17:30" \
    --params='{
        "query": "'"$(cat $SCRIPT_DIR/01_transfer_staging_to_fact.sql | sed 's/"/\\"/g' | tr '\n' ' ')"'",
        "destination_table_name_template": "",
        "write_disposition": "WRITE_TRUNCATE",
        "partitioning_field": ""
    }' 2>/dev/null || log_warn "Transfer query schedule may already exist"

# Create caption extraction query (5:35 AM and 5:35 PM)
log_info "Creating caption extraction schedule..."
bq mk --transfer_config \
    --project_id="$PROJECT_ID" \
    --location="$REGION" \
    --display_name="ETL Extract: New Captions (5:35 AM/PM)" \
    --data_source=scheduled_query \
    --target_dataset=raw \
    --schedule="every day 05:35,17:35" \
    --params='{
        "query": "'"$(cat $SCRIPT_DIR/02_extract_new_captions.sql | sed 's/"/\\"/g' | tr '\n' ' ')"'",
        "destination_table_name_template": "",
        "write_disposition": "WRITE_APPEND",
        "partitioning_field": ""
    }' 2>/dev/null || log_warn "Caption extraction schedule may already exist"

# Create username mapping query (5:40 AM and 5:40 PM)
log_info "Creating username mapping schedule..."
bq mk --transfer_config \
    --project_id="$PROJECT_ID" \
    --location="$REGION" \
    --display_name="ETL Mapping: Username Standardization (5:40 AM/PM)" \
    --data_source=scheduled_query \
    --target_dataset=raw \
    --schedule="every day 05:40,17:40" \
    --params='{
        "query": "'"$(cat $SCRIPT_DIR/03_update_username_mapping.sql | sed 's/"/\\"/g' | tr '\n' ' ')"'",
        "destination_table_name_template": "",
        "write_disposition": "WRITE_APPEND",
        "partitioning_field": ""
    }' 2>/dev/null || log_warn "Username mapping schedule may already exist"

# Step 2: Set up cron schedules
log_info "Setting up cron schedules..."
chmod +x "$SCRIPT_DIR/04_reschedule_pipelines.sh"
"$SCRIPT_DIR/04_reschedule_pipelines.sh"

# Step 3: Verify table partitioning and clustering
log_info "Verifying table configurations..."

# Check if fact_message_send has proper partitioning
log_info "Checking fact_message_send partitioning..."
bq show --format=prettyjson "$PROJECT_ID:layer_03_foundation.fact_message_send" 2>/dev/null | \
    grep -q '"field": "send_date"' && \
    log_info "fact_message_send is properly partitioned" || \
    log_warn "fact_message_send may need partitioning setup"

# Set partition filter requirement
log_info "Setting partition filter requirement..."
bq update \
    --require_partition_filter=true \
    "$PROJECT_ID:layer_03_foundation.fact_message_send" 2>/dev/null || \
    log_warn "Could not set partition filter requirement"

# Step 4: Test queries
log_info "Testing integration queries..."

# Test the transfer query (dry run)
log_info "Testing transfer query..."
if bq query --use_legacy_sql=false --dry_run --project_id="$PROJECT_ID" < "$SCRIPT_DIR/01_transfer_staging_to_fact.sql" 2>/dev/null; then
    log_info "Transfer query validated"
else
    log_error "Transfer query validation failed"
fi

# Test caption extraction query (dry run)
log_info "Testing caption extraction query..."
if bq query --use_legacy_sql=false --dry_run --project_id="$PROJECT_ID" < "$SCRIPT_DIR/02_extract_new_captions.sql" 2>/dev/null; then
    log_info "Caption extraction query validated"
else
    log_error "Caption extraction query validation failed"
fi

# Test username mapping query (dry run)
log_info "Testing username mapping query..."
if bq query --use_legacy_sql=false --dry_run --project_id="$PROJECT_ID" < "$SCRIPT_DIR/03_update_username_mapping.sql" 2>/dev/null; then
    log_info "Username mapping query validated"
else
    log_error "Username mapping query validation failed"
fi

# Step 5: Display summary
echo ""
echo "========================================="
echo "INTEGRATION DEPLOYMENT COMPLETE"
echo "========================================="
echo ""
echo "📅 Schedule Summary:"
echo "  5:00 AM/PM - Gmail ETL runs (Cloud Scheduler)"
echo "  5:30 AM/PM - Transfer staging → fact_message_send"
echo "  5:35 AM/PM - Extract new captions → caption_library"
echo "  5:40 AM/PM - Update username mappings"
echo "  6:00 AM/PM - ML Pipeline refresh"
echo ""
echo "📊 Data Flow:"
echo "  Gmail (kyle@erosops.com)"
echo "       ↓ [Python ETL]"
echo "  staging.gmail_etl_daily"
echo "       ↓ [Transfer Query]"
echo "  fact_message_send + caption_library"
echo "       ↓ [ML Pipeline]"
echo "  feature_store → rankings → recommendations"
echo ""
echo "🔍 Verification Commands:"
echo "  # Check scheduled queries:"
echo "  bq ls --transfer_config --transfer_location=$REGION --project_id=$PROJECT_ID"
echo ""
echo "  # View cron jobs:"
echo "  crontab -l"
echo ""
echo "  # Test integration manually:"
echo "  bq query --use_legacy_sql=false --project_id=$PROJECT_ID < $SCRIPT_DIR/01_transfer_staging_to_fact.sql"
echo ""
echo "  # Monitor logs:"
echo "  tail -f $(dirname $SCRIPT_DIR)/logs/*.log"
echo ""
echo "⚠️  Important Notes:"
echo "  1. Gmail ETL must complete before 5:30 AM/PM"
echo "  2. ML pipeline runs 1 hour after ETL (6:00 AM/PM)"
echo "  3. All times are in America/Denver timezone"
echo "  4. Check Cloud Scheduler for Gmail ETL status"