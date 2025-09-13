#!/bin/bash
# Test Scheduler Setup Script
# Verifies BigQuery views and data availability for Google Sheets scheduler

PROJECT_ID="of-scheduler-proj"
echo "🔍 Testing Scheduler Setup for Project: $PROJECT_ID"
echo "================================================"

# Test 1: Check if we're authenticated
echo "1️⃣ Checking BigQuery authentication..."
if bq query --project_id=$PROJECT_ID --use_legacy_sql=false "SELECT 1" >/dev/null 2>&1; then
    echo "✅ BigQuery authenticated"
else
    echo "❌ Not authenticated. Run: gcloud auth application-default login"
    exit 1
fi

# Test 2: Check project access
echo ""
echo "2️⃣ Checking project access..."
if bq ls -d --project_id=$PROJECT_ID >/dev/null 2>&1; then
    echo "✅ Can access project: $PROJECT_ID"
else
    echo "❌ Cannot access project: $PROJECT_ID"
    exit 1
fi

# Test 3: Check key datasets
echo ""
echo "3️⃣ Checking required datasets..."
DATASETS=("mart" "core" "ops" "sheets")
for dataset in "${DATASETS[@]}"; do
    if bq ls -d --project_id=$PROJECT_ID | grep -q "$dataset"; then
        echo "✅ Dataset exists: $dataset"
    else
        echo "❌ Dataset missing: $dataset"
    fi
done

# Test 4: Check critical views/tables
echo ""
echo "4️⃣ Checking critical views and tables..."

# Check caption rank table
echo -n "   Caption rank table: "
if bq show --project_id=$PROJECT_ID mart.caption_rank_next24_v3_tbl >/dev/null 2>&1; then
    ROW_COUNT=$(bq query --project_id=$PROJECT_ID --use_legacy_sql=false --format=csv \
        "SELECT COUNT(*) as cnt FROM \`$PROJECT_ID.mart.caption_rank_next24_v3_tbl\` LIMIT 1" 2>/dev/null | tail -1)
    echo "✅ Exists (${ROW_COUNT:-0} rows)"
else
    echo "❌ Missing: mart.caption_rank_next24_v3_tbl"
fi

# Check My Day All view
echo -n "   My Day All view: "
if bq show --project_id=$PROJECT_ID sheets.v_my_day_slots_all_v1 >/dev/null 2>&1; then
    echo "✅ Exists"
else
    echo "❌ Missing: sheets.v_my_day_slots_all_v1"
fi

# Check gated view
echo -n "   Gated recommendations view: "
if bq show --project_id=$PROJECT_ID mart.v_slot_recommendations_next24_gated_v1 >/dev/null 2>&1; then
    echo "✅ Exists"
else
    echo "❌ Missing: mart.v_slot_recommendations_next24_gated_v1"
fi

# Test 5: Check weekly template
echo ""
echo "5️⃣ Checking weekly template data..."
echo -n "   Weekly template pages: "
if bq show --project_id=$PROJECT_ID core.v_weekly_template_7d_pages_final >/dev/null 2>&1; then
    PAGE_COUNT=$(bq query --project_id=$PROJECT_ID --use_legacy_sql=false --format=csv \
        "SELECT COUNT(DISTINCT username_std) as cnt FROM \`$PROJECT_ID.core.v_weekly_template_7d_pages_final\` WHERE date_local = CURRENT_DATE('America/Denver')" 2>/dev/null | tail -1)
    echo "✅ Exists (${PAGE_COUNT:-0} pages today)"
else
    echo "❌ Missing: core.v_weekly_template_7d_pages_final"
fi

# Test 6: Check send_log table for logging
echo ""
echo "6️⃣ Checking send_log table..."
echo -n "   Send log table: "
if bq show --project_id=$PROJECT_ID ops.send_log >/dev/null 2>&1; then
    echo "✅ Exists (ready for logging)"
else
    echo "❌ Missing: ops.send_log"
fi

# Test 7: Sample data availability
echo ""
echo "7️⃣ Checking data availability for next 24 hours..."
CAPTION_COUNT=$(bq query --project_id=$PROJECT_ID --use_legacy_sql=false --format=csv \
    "SELECT COUNT(*) as cnt 
     FROM \`$PROJECT_ID.mart.caption_rank_next24_v3_tbl\`
     WHERE DATE(slot_dt_local) = CURRENT_DATE('America/Denver')" 2>/dev/null | tail -1)

if [ -n "$CAPTION_COUNT" ] && [ "$CAPTION_COUNT" -gt "0" ]; then
    echo "✅ Caption recommendations available: $CAPTION_COUNT slots"
else
    echo "⚠️  No caption recommendations for today - may need to run pipeline"
fi

echo ""
echo "================================================"
echo "📊 Summary:"
echo ""
echo "Next steps:"
echo "1. Open Apps Script project: https://script.google.com/d/1gCc7UoVcMDLOKiDUblKMbozB8bVGrvIZQp1FXTij6OFwNzvJ2HexnSE0/edit"
echo "2. Run 'onOpen' function to initialize sheets"
echo "3. Run 'authorizeOnce' function to authorize BigQuery"
echo "4. Test 'refreshWeekPlan' to load scheduler data"
echo "5. Share the spreadsheet with schedulers (viewer access)"
echo ""
echo "✨ Google Sheets Scheduler Hub ready for testing!"