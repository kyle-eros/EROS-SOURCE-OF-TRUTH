#!/usr/bin/env bash

# ==============================================================================
# BIGQUERY ORGANIZED AUDIT SCRIPT (MULTI-FILE OUTPUT) - FIXED VERSION
# ==============================================================================
# This script inspects a Google BigQuery project and generates a structured
# audit with organized file output instead of one large JSON file.
# ==============================================================================

PROJECT_ID="${1:-"of-scheduler-proj"}"
OUTPUT_DIR="bigquery_audit_$(date +%Y%m%d_%H%M%S)"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

set -euo pipefail

# Create output directory structure
mkdir -p "$OUTPUT_DIR"/{project,datasets}

echo "🔍 Starting organized BigQuery audit for project: $PROJECT_ID"
echo "📁 Output directory: $OUTPUT_DIR"

# Validate that required command-line tools (bq, jq) are installed.
for tool in bq jq; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Error: Required command '$tool' not found." >&2
    exit 1
  fi
done

# Helper function to safely execute bq commands and return valid JSON.
bq_json() {
  local output
  if ! output=$(bq "$@" 2>&1); then
    echo "[]"
    return
  fi
  if ! <<<"$output" jq -e . >/dev/null 2>&1; then
    echo "[]"
    return
  fi
  echo "$output"
}

# Helper function to write JSON files with pretty formatting
write_json() {
    local filepath="$1"
    local data="$2"
    mkdir -p "$(dirname "$filepath")"
    echo "$data" | jq '.' > "$filepath"
}

# Initialize counters for summary
TOTAL_DATASETS=0
TOTAL_TABLES=0
TOTAL_VIEWS=0
TOTAL_MAT_VIEWS=0
TOTAL_ROUTINES=0
TOTAL_SCHEDULED_QUERIES=0

# Create project metadata
PROJECT_METADATA=$(jq -n \
  --arg project_id "$PROJECT_ID" \
  --arg timestamp "$TIMESTAMP" \
  --arg generated_by "bq_organized_audit_script_v2" \
  '{
    project_id: $project_id,
    timestamp: $timestamp,
    generated_by: $generated_by,
    audit_version: "2.0"
  }')

write_json "$OUTPUT_DIR/project/metadata.json" "$PROJECT_METADATA"

# --- 1. Collect Scheduled Queries ---
echo "📅 Collecting scheduled queries..."
LOCATIONS=("us" "us-central1" "us-east1" "us-west1" "eu" "europe-west1")
ALL_SCHEDULES="[]"

for location in "${LOCATIONS[@]}"; do
    echo "  -> Checking location: $location"
    SCHEDULES=$(bq_json --project_id="$PROJECT_ID" ls --transfer_config --transfer_location="$location" --format=json)
    ALL_SCHEDULES=$(jq -s 'add' <(echo "$ALL_SCHEDULES") <(echo "$SCHEDULES"))
done

# Process and format scheduled query data
PROCESSED_SCHEDULES=$(echo "$ALL_SCHEDULES" | jq '[.[] | {
  name: .displayName,
  schedule: .schedule,
  next_run: .nextRunTime,
  location: .dataLocationId,
  query: .params.query,
  destination_table: (.params.destination_table_name_template // "N/A"),
  created: .updateTime,
  enabled: (.state == "ENABLED")
}]')

TOTAL_SCHEDULED_QUERIES=$(echo "$PROCESSED_SCHEDULES" | jq 'length')
TOTAL_SCHEDULED_QUERIES=${TOTAL_SCHEDULED_QUERIES:-0}
write_json "$OUTPUT_DIR/project/scheduled_queries.json" "$PROCESSED_SCHEDULES"

# --- 2. Process Datasets ---
echo "📂 Collecting datasets and organizing by dataset..."
DATASETS_JSON=$(bq_json --project_id="$PROJECT_ID" ls --datasets --format=json)

# Process each dataset
echo "$DATASETS_JSON" | jq -c '.[]' | while read -r dataset; do
  DATASET_ID=$(echo "$dataset" | jq -r '.datasetId // .datasetReference.datasetId')
  echo "  -> Processing dataset: $DATASET_ID"
  
  # Create dataset directory
  DATASET_DIR="$OUTPUT_DIR/datasets/$DATASET_ID"
  mkdir -p "$DATASET_DIR"
  
  # Get dataset metadata
  DATASET_INFO=$(bq_json --project_id="$PROJECT_ID" show --format=json "$DATASET_ID")
  
  # Format dataset info
  DATASET_METADATA=$(echo "$DATASET_INFO" | jq '{
    id: .datasetReference.datasetId,
    description: (.description // ""),
    location: (.location // ""),
    created: ((.creationTime // 0 | tonumber) / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
    modified: ((.lastModifiedTime // 0 | tonumber) / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
    access_entries: (.access // [])
  }')
  
  write_json "$DATASET_DIR/info.json" "$DATASET_METADATA"
  
  # List all objects in the dataset
  OBJECTS=$(bq_json --project_id="$PROJECT_ID" ls --max_results=10000 --format=json "$DATASET_ID")
  
  # --- Process Tables ---
  echo "    -> Processing tables..."
  TABLES_DATA="[]"
  
  while read -r table_ref; do
    if [[ -n "$table_ref" ]]; then
      TABLE_ID=$(echo "$table_ref" | jq -r '.tableReference.tableId')
      echo "      -> Table: $TABLE_ID"
      TABLE_INFO=$(bq_json show --format=json "$PROJECT_ID:$DATASET_ID.$TABLE_ID")
      if [[ -n "$TABLE_INFO" && "$TABLE_INFO" != "[]" ]]; then
        TABLE_DATA=$(echo "$TABLE_INFO" | jq '{
          id: .tableReference.tableId,
          type: "TABLE",
          description: (.description // ""),
          created: (.creationTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          modified: (.lastModifiedTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          size_bytes: (.numBytes // 0 | tonumber),
          num_rows: (.numRows // 0 | tonumber),
          schema: (.schema.fields // [])
        }')
        TABLES_DATA=$(echo "$TABLES_DATA" | jq --argjson new_table "$TABLE_DATA" '. + [$new_table]')
      fi
    fi
  done < <(echo "$OBJECTS" | jq -c '.[] | select(.type == "TABLE")')
  
  write_json "$DATASET_DIR/tables.json" "$TABLES_DATA"
  
  # --- Process Views ---
  echo "    -> Processing views..."
  VIEWS_DATA="[]"
  
  while read -r view_ref; do
    if [[ -n "$view_ref" ]]; then
      VIEW_ID=$(echo "$view_ref" | jq -r '.tableReference.tableId')
      echo "      -> View: $VIEW_ID"
      VIEW_INFO=$(bq_json show --format=json "$PROJECT_ID:$DATASET_ID.$VIEW_ID")
      if [[ -n "$VIEW_INFO" && "$VIEW_INFO" != "[]" ]]; then
        VIEW_DATA=$(echo "$VIEW_INFO" | jq '{
          id: .tableReference.tableId,
          type: "VIEW",
          description: (.description // ""),
          created: (.creationTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          modified: (.lastModifiedTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          query: .view.query,
          schema: (.schema.fields // [])
        }')
        VIEWS_DATA=$(echo "$VIEWS_DATA" | jq --argjson new_view "$VIEW_DATA" '. + [$new_view]')
      fi
    fi
  done < <(echo "$OBJECTS" | jq -c '.[] | select(.type == "VIEW")')
  
  write_json "$DATASET_DIR/views.json" "$VIEWS_DATA"
  
  # --- Process Materialized Views ---
  echo "    -> Processing materialized views..."
  MAT_VIEWS_DATA="[]"
  
  while read -r mv_ref; do
    if [[ -n "$mv_ref" ]]; then
      MV_ID=$(echo "$mv_ref" | jq -r '.tableReference.tableId')
      echo "      -> Materialized View: $MV_ID"
      MV_INFO=$(bq_json show --format=json "$PROJECT_ID:$DATASET_ID.$MV_ID")
      if [[ -n "$MV_INFO" && "$MV_INFO" != "[]" ]]; then
        MV_DATA=$(echo "$MV_INFO" | jq '{
          id: .tableReference.tableId,
          type: "MATERIALIZED_VIEW",
          description: (.description // ""),
          created: (.creationTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          modified: (.lastModifiedTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          size_bytes: (.numBytes // 0 | tonumber),
          num_rows: (.numRows // 0 | tonumber),
          query: .materializedView.query,
          schema: (.schema.fields // [])
        }')
        MAT_VIEWS_DATA=$(echo "$MAT_VIEWS_DATA" | jq --argjson new_mv "$MV_DATA" '. + [$new_mv]')
      fi
    fi
  done < <(echo "$OBJECTS" | jq -c '.[] | select(.type == "MATERIALIZED_VIEW")')
  
  write_json "$DATASET_DIR/materialized_views.json" "$MAT_VIEWS_DATA"
  
  # --- Process Routines ---
  echo "    -> Processing routines..."
  ROUTINES_DATA="[]"
  
  ROUTINES_LIST=$(bq_json --project_id="$PROJECT_ID" ls --routines --format=json "$DATASET_ID")
  while read -r routine_ref; do
    if [[ -n "$routine_ref" ]]; then
      ROUTINE_ID=$(echo "$routine_ref" | jq -r '.routineReference.routineId')
      echo "      -> Routine: $ROUTINE_ID"
      ROUTINE_INFO=$(bq_json show --format=json --routine "$PROJECT_ID:$DATASET_ID.$ROUTINE_ID")
      if [[ -n "$ROUTINE_INFO" && "$ROUTINE_INFO" != "[]" ]]; then
        ROUTINE_DATA=$(echo "$ROUTINE_INFO" | jq '{
          id: .routineReference.routineId,
          type: .routineType,
          language: (.language // "SQL"),
          created: (.creationTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          modified: (.lastModifiedTime // 0 | tonumber / 1000 | strftime("%Y-%m-%d %H:%M:%S")),
          arguments: (.arguments // []),
          return_type: (.returnType // null),
          definition: .definitionBody
        }')
        ROUTINES_DATA=$(echo "$ROUTINES_DATA" | jq --argjson new_routine "$ROUTINE_DATA" '. + [$new_routine]')
      fi
    fi
  done < <(echo "$ROUTINES_LIST" | jq -c '.[]')
  
  write_json "$DATASET_DIR/routines.json" "$ROUTINES_DATA"
  
  # Count objects in this dataset
  TABLES_COUNT=$(echo "$TABLES_DATA" | jq 'length')
  VIEWS_COUNT=$(echo "$VIEWS_DATA" | jq 'length')
  MAT_VIEWS_COUNT=$(echo "$MAT_VIEWS_DATA" | jq 'length')
  ROUTINES_COUNT=$(echo "$ROUTINES_DATA" | jq 'length')
  
  # Create dataset summary
  DATASET_SUMMARY=$(jq -n \
    --arg dataset_id "$DATASET_ID" \
    --argjson tables_count "$TABLES_COUNT" \
    --argjson views_count "$VIEWS_COUNT" \
    --argjson mat_views_count "$MAT_VIEWS_COUNT" \
    --argjson routines_count "$ROUTINES_COUNT" \
    '{
      dataset_id: $dataset_id,
      counts: {
        tables: $tables_count,
        views: $views_count,
        materialized_views: $mat_views_count,
        routines: $routines_count
      }
    }')
  
  write_json "$DATASET_DIR/summary.json" "$DATASET_SUMMARY"
  
  echo "    -> Dataset $DATASET_ID complete (T:$TABLES_COUNT, V:$VIEWS_COUNT, MV:$MAT_VIEWS_COUNT, R:$ROUTINES_COUNT)"
done

# Calculate final totals by reading the dataset summary files
echo "📊 Calculating final summary..."
TOTAL_DATASETS=$(find "$OUTPUT_DIR/datasets" -name "summary.json" 2>/dev/null | wc -l | tr -d ' ')
TOTAL_DATASETS=${TOTAL_DATASETS:-0}
TOTAL_TABLES=$(find "$OUTPUT_DIR/datasets" -name "summary.json" -exec jq -r '.counts.tables' {} + 2>/dev/null | paste -sd+ - | bc 2>/dev/null || echo 0)
TOTAL_TABLES=${TOTAL_TABLES:-0}
TOTAL_VIEWS=$(find "$OUTPUT_DIR/datasets" -name "summary.json" -exec jq -r '.counts.views' {} + 2>/dev/null | paste -sd+ - | bc 2>/dev/null || echo 0)
TOTAL_VIEWS=${TOTAL_VIEWS:-0}
TOTAL_MAT_VIEWS=$(find "$OUTPUT_DIR/datasets" -name "summary.json" -exec jq -r '.counts.materialized_views' {} + 2>/dev/null | paste -sd+ - | bc 2>/dev/null || echo 0)
TOTAL_MAT_VIEWS=${TOTAL_MAT_VIEWS:-0}
TOTAL_ROUTINES=$(find "$OUTPUT_DIR/datasets" -name "summary.json" -exec jq -r '.counts.routines' {} + 2>/dev/null | paste -sd+ - | bc 2>/dev/null || echo 0)
TOTAL_ROUTINES=${TOTAL_ROUTINES:-0}

# Create project summary
PROJECT_SUMMARY=$(jq -n \
  --argjson total_datasets "$TOTAL_DATASETS" \
  --argjson total_tables "$TOTAL_TABLES" \
  --argjson total_views "$TOTAL_VIEWS" \
  --argjson total_materialized_views "$TOTAL_MAT_VIEWS" \
  --argjson total_routines "$TOTAL_ROUTINES" \
  --argjson total_scheduled_queries "$TOTAL_SCHEDULED_QUERIES" \
  '{
    total_datasets: $total_datasets,
    total_tables: $total_tables,
    total_views: $total_views,
    total_materialized_views: $total_materialized_views,
    total_routines: $total_routines,
    total_scheduled_queries: $total_scheduled_queries
  }')

write_json "$OUTPUT_DIR/project/summary.json" "$PROJECT_SUMMARY"

# Create README file
cat > "$OUTPUT_DIR/README.md" << EOF
# BigQuery Audit Report

**Project:** $PROJECT_ID  
**Generated:** $(date)  
**Audit Type:** Organized Multi-File Output

## Summary
- **Datasets:** $TOTAL_DATASETS
- **Tables:** $TOTAL_TABLES
- **Views:** $TOTAL_VIEWS
- **Materialized Views:** $TOTAL_MAT_VIEWS
- **Routines:** $TOTAL_ROUTINES
- **Scheduled Queries:** $TOTAL_SCHEDULED_QUERIES

## File Structure

\`\`\`
$OUTPUT_DIR/
├── project/
│   ├── metadata.json          # Project info and audit metadata
│   ├── scheduled_queries.json # All scheduled queries
│   └── summary.json          # Overall counts and statistics
├── datasets/
│   ├── [dataset_name]/
│   │   ├── info.json         # Dataset metadata and access control
│   │   ├── tables.json       # All tables with schema and stats
│   │   ├── views.json        # All views with definitions
│   │   ├── materialized_views.json # Materialized views
│   │   ├── routines.json     # Functions and procedures
│   │   └── summary.json      # Dataset-level counts
└── README.md                 # This file
\`\`\`

## Usage Examples

### List all datasets
\`\`\`bash
ls datasets/
\`\`\`

### Find all tables across datasets
\`\`\`bash
find datasets -name "tables.json" -exec jq -r '.[].id' {} +
\`\`\`

### Get total row count for all tables
\`\`\`bash
find datasets -name "tables.json" -exec jq -r '.[].num_rows' {} + | paste -sd+ - | bc
\`\`\`

### Find views that reference a specific table
\`\`\`bash
find datasets -name "views.json" -exec grep -l "your_table_name" {} +
\`\`\`

## Benefits of This Structure
- **Manageable file sizes**: No single large file to overwhelm editors
- **Easy navigation**: Find specific datasets and objects quickly
- **Selective processing**: Work with individual datasets independently
- **Scalable**: Handles projects with hundreds of datasets efficiently
- **Tool-friendly**: Each JSON file can be processed by standard tools

EOF

echo "✅ BigQuery organized audit complete!"
echo "📁 Output directory: $OUTPUT_DIR"
echo ""
echo "📊 Final Summary:"
echo "  Datasets: $TOTAL_DATASETS"
echo "  Tables: $TOTAL_TABLES"
echo "  Views: $TOTAL_VIEWS"
echo "  Materialized Views: $TOTAL_MAT_VIEWS"
echo "  Routines: $TOTAL_ROUTINES"
echo "  Scheduled Queries: $TOTAL_SCHEDULED_QUERIES"
echo ""
echo "📖 See README.md for usage examples and file structure details"