#!/bin/bash

# List of views to process
VIEWS=(
  "caption_ranker_vNext"
  "v_caption_rank_next24_v3"
  "v_daily_brief_today"
  "v_dm_style_lift_28d_v3"
  "v_non_dm_windows_7d_v1"
  "v_send_to_perf_link_180d"
  "v_slot_recommendations_next24_gated_v1"
  "v_weekly_template_7d_pages_final"
  "v_weekly_template_7d_pages_overrides"
  "v_weekly_template_7d_v7"
  "v_weekly_template_audit"
)

echo "Starting migration of 11 remaining views..."
echo "=========================================="
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0

for VIEW_NAME in "${VIEWS[@]}"; do
    echo "Processing: $VIEW_NAME"
    echo "------------------------"

    # Step 1: Dump current definition
    echo "  1. Dumping definition..."
    bq show --format=prettyjson of-scheduler-proj:mart.$VIEW_NAME > migration/auto/${VIEW_NAME}_v2.json 2>&1

    # Step 2: Extract query
    echo "  2. Extracting query..."
    cat migration/auto/${VIEW_NAME}_v2.json | python3 -c "import json, sys; d = json.load(sys.stdin); print(d.get('view', {}).get('query', ''))" > migration/auto/${VIEW_NAME}_v2.orig.sql 2>/dev/null

    # Step 3: Apply mappings
    echo "  3. Applying mappings..."
    python3 migration/auto/apply_mappings_v2.py migration/auto/mappings_complete.json migration/auto/${VIEW_NAME}_v2.orig.sql migration/auto/${VIEW_NAME}_v2.patched.sql

    # Step 4: Create apply SQL
    echo "  4. Creating apply SQL..."
    echo "CREATE OR REPLACE VIEW \`of-scheduler-proj.mart.$VIEW_NAME\` AS" > migration/auto/${VIEW_NAME}_v2.apply.sql
    cat migration/auto/${VIEW_NAME}_v2.patched.sql >> migration/auto/${VIEW_NAME}_v2.apply.sql

    # Step 5: Dry-run
    echo "  5. Testing dry-run..."
    if bq --location=US query --use_legacy_sql=false --dry_run < migration/auto/${VIEW_NAME}_v2.apply.sql 2> migration/auto/${VIEW_NAME}_v2.error.txt; then
        echo "  ✓ Dry-run successful"

        # Step 6: Apply
        echo "  6. Applying migration..."
        if bq --location=US query --use_legacy_sql=false < migration/auto/${VIEW_NAME}_v2.apply.sql 2>&1 | grep -q "Replaced\|Created"; then
            echo "  ✓ Migration applied successfully"

            # Step 7: Verify
            echo "  7. Verifying no core references..."
            echo "SELECT REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\\.core\\.') AS has_core" > migration/auto/${VIEW_NAME}_v2.verify.sql
            echo "FROM \`of-scheduler-proj.mart.INFORMATION_SCHEMA.VIEWS\`" >> migration/auto/${VIEW_NAME}_v2.verify.sql
            echo "WHERE table_name = '$VIEW_NAME';" >> migration/auto/${VIEW_NAME}_v2.verify.sql

            RESULT=$(bq --location=US query --use_legacy_sql=false --format=csv < migration/auto/${VIEW_NAME}_v2.verify.sql 2>/dev/null | tail -1)
            if [ "$RESULT" = "false" ]; then
                echo "  ✓ Verified: No core references"
                echo "$VIEW_NAME: SUCCESS" >> migration/auto/results_v2.txt
                ((SUCCESS_COUNT++))
            else
                echo "  ⚠ Warning: Still has core references"
                echo "$VIEW_NAME: PARTIAL - Still has core refs" >> migration/auto/results_v2.txt
                ((FAIL_COUNT++))
            fi
        else
            echo "  ✗ Failed to apply migration"
            echo "$VIEW_NAME: FAILED - Apply failed" >> migration/auto/results_v2.txt
            ((FAIL_COUNT++))
        fi
    else
        echo "  ✗ Dry-run failed (see ${VIEW_NAME}_v2.error.txt)"
        ERROR_MSG=$(head -1 migration/auto/${VIEW_NAME}_v2.error.txt)
        echo "$VIEW_NAME: FAILED - $ERROR_MSG" >> migration/auto/results_v2.txt
        ((FAIL_COUNT++))
    fi

    echo ""
done

echo "=========================================="
echo "Migration Summary:"
echo "  Successful: $SUCCESS_COUNT"
echo "  Failed: $FAIL_COUNT"
echo ""
echo "Detailed Results:"
cat migration/auto/results_v2.txt