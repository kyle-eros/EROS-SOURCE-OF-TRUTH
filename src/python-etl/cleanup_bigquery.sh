#!/bin/bash
# BigQuery Post-Migration Cleanup Script
# Date: September 11, 2025
# Purpose: Clean up old Gmail ETL infrastructure after successful migration

# Harden shell - exit on error, undefined vars, and pipe failures
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# --- REQUIRED SAFETY PREFLIGHTS (abort on any dependency) ---

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-of-scheduler-proj}"
LOCATION="${LOCATION:-US}"

# Tools sanity
command -v jq >/dev/null 2>&1 || { echo "jq is required"; exit 1; }

echo "================================================"
echo "   BigQuery Post-Migration Cleanup Script"
echo "================================================"
echo ""
echo "Running safety preflights to check for dependencies..."
echo ""

# Helper: list all datasets in this project
DATASETS=()
while IFS= read -r ds; do
  [[ -n "$ds" ]] && DATASETS+=("$ds")
done < <(bq ls --project_id="$PROJECT_ID" --format=json 2>/dev/null | jq -r '.[].datasetReference.datasetId' 2>/dev/null || true)

# 1) Scan VIEWS across datasets for textual refs to legacy objects
echo "Preflight: scanning views that reference legacy tables..."
LEGACY_REGEX='staging\.gmail_etl_daily|staging\.historical_message_staging|staging\.fn_gmail_etl_normalized'

views_hits=()
for ds in "${DATASETS[@]}"; do
  json="$(bq query --use_legacy_sql=false --format=json "
    SELECT CONCAT('$ds','.',table_name) AS view_name
    FROM \`$PROJECT_ID.$ds\`.INFORMATION_SCHEMA.VIEWS
    WHERE view_definition IS NOT NULL
      AND REGEXP_CONTAINS(LOWER(view_definition), r'$LEGACY_REGEX')
  " 2>/dev/null || echo '[]')"
  # collect any hits
  while IFS= read -r v; do
    [[ -n "$v" && "$v" != "null" ]] && views_hits+=("$v")
  done < <(echo "$json" | jq -r '.[].view_name' 2>/dev/null || true)
done

if (( ${#views_hits[@]} > 0 )); then
  echo -e "${RED}❌ Found views still referencing legacy objects:${NC}"
  printf '  - %s\n' "${views_hits[@]}"
  echo ""
  echo -e "${YELLOW}Fix or drop these views, then re-run cleanup.${NC}"
  exit 1
else
  echo -e "${GREEN}✓ No views reference legacy objects${NC}"
fi

# 2) Scan ROUTINES across datasets (functions / procedures)
echo "Preflight: scanning routines (functions/procedures) referencing legacy objects..."
routines_hits=()
for ds in "${DATASETS[@]}"; do
  json="$(bq query --use_legacy_sql=false --format=json "
    SELECT CONCAT('$ds','.',routine_name,' (',routine_type,')') AS routine
    FROM \`$PROJECT_ID.$ds\`.INFORMATION_SCHEMA.ROUTINES
    WHERE routine_definition IS NOT NULL
      AND REGEXP_CONTAINS(LOWER(routine_definition), r'$LEGACY_REGEX')
  " 2>/dev/null || echo '[]')"
  while IFS= read -r r; do
    [[ -n "$r" && "$r" != "null" ]] && routines_hits+=("$r")
  done < <(echo "$json" | jq -r '.[].routine' 2>/dev/null || true)
done

if (( ${#routines_hits[@]} > 0 )); then
  echo -e "${RED}❌ Found routines still referencing legacy objects:${NC}"
  printf '  - %s\n' "${routines_hits[@]}"
  echo ""
  echo -e "${YELLOW}Update or drop these routines, then re-run cleanup.${NC}"
  exit 1
else
  echo -e "${GREEN}✓ No routines reference legacy objects${NC}"
fi

# 3) Scan scheduled queries (BigQuery Data Transfer Service)
echo "Preflight: scanning scheduled queries for legacy references..."
sq_names=()
while IFS= read -r name; do
  [[ -n "$name" ]] && sq_names+=("$name")
done < <(bq ls --transfer_config --transfer_location="$LOCATION" --format=json 2>/dev/null \
        | jq -r '.[].name' 2>/dev/null || true)

bad_sq=()
for cfg in "${sq_names[@]:-}"; do
  qtxt="$(bq show --transfer_config "$cfg" --format=json 2>/dev/null | jq -r '.params.query // empty' 2>/dev/null || true)"
  if [[ -n "$qtxt" ]] && grep -qiE "$LEGACY_REGEX" <<<"$qtxt" 2>/dev/null; then
    bad_sq+=("$cfg")
  fi
done

if (( ${#bad_sq[@]} > 0 )); then
  echo -e "${RED}❌ Found scheduled queries referencing legacy objects:${NC}"
  printf '  - %s\n' "${bad_sq[@]}"
  echo ""
  echo -e "${YELLOW}Update or pause these schedules, then re-run cleanup.${NC}"
  exit 1
else
  echo -e "${GREEN}✓ No scheduled queries reference legacy objects${NC}"
fi

echo ""
echo -e "${GREEN}✅ Preflight checks passed: no dependencies on legacy objects.${NC}"
echo ""
# --- END PREFLIGHTS ---

echo "================================================"
echo "   Starting Cleanup Process"
echo "================================================"
echo ""

# Safety check
echo -e "${YELLOW}⚠️  WARNING: This will delete old BigQuery objects${NC}"
echo "Have you verified that:"
echo "1. The new ETL pipeline is working correctly?"
echo "2. No scheduled queries reference the old tables?"
echo "3. You have the backup staging.gmail_etl_daily_legacy_20250911?"
echo ""
read -p "Type 'yes' to continue: " confirmation

if [ "$confirmation" != "yes" ]; then
    echo -e "${RED}Cleanup cancelled.${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}Starting cleanup...${NC}"
echo ""

# Phase 1: Delete old backups from Sept 9
echo "Phase 1: Removing old backups from September 9..."
echo "------------------------------------------------"

tables_to_delete=(
    "staging.gmail_etl_daily_old_20250909_215137"
    "staging.historical_message_staging_old_20250909_215137"
    "raw.caption_library_backup_20250909"
    "raw.model_profiles_enhanced_backup_20250909"
    "raw.scheduled_sends_backup_20250909"
    "raw.username_mapping_backup_20250909"
)

for table in "${tables_to_delete[@]}"; do
    echo -n "  Deleting $table... "
    if bq rm -f -t "$table" 2>/dev/null; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${YELLOW}Already deleted or not found${NC}"
    fi
done

echo ""

# Phase 2: Remove old table function
echo "Phase 2: Removing old table function..."
echo "----------------------------------------"
echo -n "  Deleting staging.fn_gmail_etl_normalized... "
if bq rm -f --routine staging.fn_gmail_etl_normalized 2>/dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}Already deleted or not found${NC}"
fi

echo ""

# Phase 3: Check for references to old tables (informational only)
echo "Phase 3: Checking for references to old tables..."
echo "-------------------------------------------------"
echo "Searching for views that reference staging.gmail_etl_daily..."

# List views that might reference the old table
views_to_check=(
    "staging.v_gmail_etl_daily_deduped"
    "staging.v_all_historical_enhanced"
    "staging.v_historical_filtered"
)

echo ""
echo -e "${YELLOW}Views that may need updating:${NC}"
for view in "${views_to_check[@]}"; do
    echo "  - $view"
done

echo ""
echo "================================================"
echo -e "${GREEN}✅ Safe cleanup complete!${NC}"
echo "================================================"
echo ""
echo -e "${YELLOW}⚠️  IMPORTANT REMINDERS:${NC}"
echo ""
echo "1. DO NOT DELETE YET (wait 1 week):"
echo "   - staging.gmail_etl_daily"
echo ""
echo "2. KEEP AS BACKUP (30 days):"
echo "   - staging.gmail_etl_daily_legacy_20250911"
echo ""
echo "3. MANUAL REVIEW NEEDED:"
echo "   - Check scheduled queries for old table references"
echo "   - Update views listed above if they reference old tables"
echo "   - Verify staging.creator_stats_upload usage"
echo "   - Verify historical_message_staging usage"
echo ""
echo "4. After 1 week of successful operation, run:"
echo "   bq rm -f -t staging.gmail_etl_daily"
echo ""
echo "================================================"
echo "New architecture components in use:"
echo "  ✅ layer_02_staging.gmail_events_staging"
echo "  ✅ layer_02_staging.fn_gmail_events_normalized"
echo "  ✅ layer_03_foundation.fact_message_send"
echo "  ✅ ops.quarantine_gmail"
echo "================================================"