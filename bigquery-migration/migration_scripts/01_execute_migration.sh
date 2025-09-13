#!/bin/bash

# =========================================
# BigQuery ML Architecture Migration Script
# =========================================
# Purpose: Execute the complete migration to new architecture
# Run each layer in sequence with validation
# =========================================

set -e  # Exit on error

PROJECT_ID="of-scheduler-proj"
MIGRATION_DIR="$(dirname "$0")/.."

echo "========================================="
echo "Starting BigQuery ML Architecture Migration"
echo "Project: $PROJECT_ID"
echo "Timestamp: $(date)"
echo "========================================="

# Function to run SQL file
run_sql() {
    local sql_file=$1
    local description=$2
    
    echo ""
    echo "Executing: $description"
    echo "File: $sql_file"
    
    if [ -f "$sql_file" ]; then
        bq query --use_legacy_sql=false --project_id=$PROJECT_ID < "$sql_file"
        echo "✓ Completed: $description"
    else
        echo "⚠ Skipped: File not found - $sql_file"
    fi
}

# Function to validate table creation
validate_table() {
    local dataset=$1
    local table=$2
    
    if bq show --project_id=$PROJECT_ID "${dataset}.${table}" > /dev/null 2>&1; then
        echo "  ✓ Validated: ${dataset}.${table}"
        return 0
    else
        echo "  ✗ Missing: ${dataset}.${table}"
        return 1
    fi
}

# =========================================
# PHASE 1: Foundation Layer
# =========================================
echo ""
echo "========================================="
echo "PHASE 1: Building Foundation Layer"
echo "========================================="

run_sql "$MIGRATION_DIR/03_foundation/01_dim_caption.sql" "Creating Caption Dimension"
run_sql "$MIGRATION_DIR/03_foundation/02_dim_creator.sql" "Creating Creator Dimension"
run_sql "$MIGRATION_DIR/03_foundation/03_fact_message_send.sql" "Creating Message Send Fact Table"

# Validate foundation tables
echo ""
echo "Validating Foundation Layer..."
validate_table "layer_03_foundation" "dim_caption"
validate_table "layer_03_foundation" "dim_creator"
validate_table "layer_03_foundation" "fact_message_send"

# =========================================
# PHASE 2: Staging Layer
# =========================================
echo ""
echo "========================================="
echo "PHASE 2: Building Staging Layer"
echo "========================================="

run_sql "$MIGRATION_DIR/02_staging/01_stg_message_events.sql" "Creating Staging Message Events"

# =========================================
# PHASE 3: Semantic Layer
# =========================================
echo ""
echo "========================================="
echo "PHASE 3: Building Semantic Layer"
echo "========================================="

run_sql "$MIGRATION_DIR/04_semantic/01_caption_performance_daily.sql" "Creating Caption Performance Daily"

# =========================================
# PHASE 4: ML Layer
# =========================================
echo ""
echo "========================================="
echo "PHASE 4: Building ML Feature Store"
echo "========================================="

run_sql "$MIGRATION_DIR/05_ml/01_feature_store.sql" "Creating ML Feature Store"
run_sql "$MIGRATION_DIR/05_ml/02_ml_ranker.sql" "Creating ML Ranker View"

# =========================================
# PHASE 5: Export Layer
# =========================================
echo ""
echo "========================================="
echo "PHASE 5: Building Export Layer"
echo "========================================="

run_sql "$MIGRATION_DIR/07_export/01_schedule_recommendations.sql" "Creating Schedule Recommendations"
run_sql "$MIGRATION_DIR/07_export/02_api_caption_lookup.sql" "Creating API Caption Lookup"

# =========================================
# PHASE 6: Operational Tables
# =========================================
echo ""
echo "========================================="
echo "PHASE 6: Setting Up Operational Tables"
echo "========================================="

run_sql "$MIGRATION_DIR/ops_config/01_ml_parameters.sql" "Creating ML Parameters Config"
run_sql "$MIGRATION_DIR/ops_monitor/01_data_quality_checks.sql" "Creating Data Quality Monitoring"

# =========================================
# Final Validation
# =========================================
echo ""
echo "========================================="
echo "FINAL VALIDATION"
echo "========================================="

echo "Checking all critical tables..."
validate_table "layer_03_foundation" "dim_caption"
validate_table "layer_03_foundation" "dim_creator"
validate_table "layer_03_foundation" "fact_message_send"
validate_table "layer_04_semantic" "caption_performance_daily"
validate_table "layer_05_ml" "feature_store"
validate_table "layer_07_export" "schedule_recommendations"
validate_table "ops_config" "ml_parameters"

echo ""
echo "========================================="
echo "✓ MIGRATION COMPLETE!"
echo "========================================="
echo ""
echo "Next Steps:"
echo "1. Run validation queries to verify data integrity"
echo "2. Update Apps Script to use new export views"
echo "3. Monitor data quality dashboard"
echo "4. Set up scheduled refreshes for materialized tables"
echo ""
echo "To rollback, restore from backup_20250911_pre_migration dataset"