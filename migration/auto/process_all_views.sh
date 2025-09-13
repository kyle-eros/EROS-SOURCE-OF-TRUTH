#!/bin/bash

# Process all views
echo "Starting migration of all views..."
echo "================================"

# Read views from todo file
while IFS= read -r VIEW_NAME; do
    if [ -z "$VIEW_NAME" ]; then
        continue
    fi

    echo ""
    echo "Processing: $VIEW_NAME"
    echo "------------------------"

    # Step 1: Dump definition
    echo "  1. Dumping definition..."
    bq show --format=prettyjson of-scheduler-proj:mart.$VIEW_NAME > migration/auto/${VIEW_NAME}.json 2>&1

    # Step 2: Extract query
    echo "  2. Extracting query..."
    cat migration/auto/${VIEW_NAME}.json | python3 -c "import json, sys; d = json.load(sys.stdin); print(d.get('view', {}).get('query', ''))" > migration/auto/${VIEW_NAME}.orig.sql 2>/dev/null

    # Step 3: Apply mappings
    echo "  3. Applying mappings..."
    python3 migration/auto/apply_mappings.py migration/auto/mappings.json migration/auto/${VIEW_NAME}.orig.sql migration/auto/${VIEW_NAME}.patched.sql

    # Step 4: Create apply SQL
    echo "  4. Creating apply SQL..."
    echo "CREATE OR REPLACE VIEW \`of-scheduler-proj.mart.$VIEW_NAME\` AS" > migration/auto/${VIEW_NAME}.apply.sql
    cat migration/auto/${VIEW_NAME}.patched.sql >> migration/auto/${VIEW_NAME}.apply.sql

    # Step 5: Dry-run
    echo "  5. Testing dry-run..."
    if bq --location=US query --use_legacy_sql=false --dry_run < migration/auto/${VIEW_NAME}.apply.sql 2> migration/auto/${VIEW_NAME}.error.txt; then
        echo "  ✓ Dry-run successful"

        # Step 6: Apply
        echo "  6. Applying migration..."
        if bq --location=US query --use_legacy_sql=false < migration/auto/${VIEW_NAME}.apply.sql 2>&1 | grep -q "Replaced"; then
            echo "  ✓ Migration applied successfully"

            # Step 7: Verify
            echo "  7. Verifying no core references..."
            echo "SELECT REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\\.core\\.') AS has_core" > migration/auto/${VIEW_NAME}.verify.sql
            echo "FROM \`of-scheduler-proj.mart.INFORMATION_SCHEMA.VIEWS\`" >> migration/auto/${VIEW_NAME}.verify.sql
            echo "WHERE table_name = '$VIEW_NAME';" >> migration/auto/${VIEW_NAME}.verify.sql

            RESULT=$(bq --location=US query --use_legacy_sql=false --format=csv < migration/auto/${VIEW_NAME}.verify.sql 2>/dev/null | tail -1)
            if [ "$RESULT" = "false" ]; then
                echo "  ✓ Verified: No core references"
                echo "$VIEW_NAME: SUCCESS" >> migration/auto/results.txt
            else
                echo "  ⚠ Warning: Still has core references"
                echo "$VIEW_NAME: PARTIAL - Still has core refs" >> migration/auto/results.txt
            fi
        else
            echo "  ✗ Failed to apply migration"
            echo "$VIEW_NAME: FAILED - Apply failed" >> migration/auto/results.txt
        fi
    else
        echo "  ✗ Dry-run failed (see ${VIEW_NAME}.error.txt)"
        echo "$VIEW_NAME: FAILED - $(head -1 migration/auto/${VIEW_NAME}.error.txt)" >> migration/auto/results.txt
    fi

done < migration/auto/todo_views.txt

echo ""
echo "================================"
echo "Migration complete. Results:"
echo ""
cat migration/auto/results.txt