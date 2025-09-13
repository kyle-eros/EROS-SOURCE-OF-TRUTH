#!/bin/bash
# =====================================================
# HISTORICAL DATA LOADING SCRIPT
# Safely load and filter MASS.MESSAGE.STATs.csv
# =====================================================

set -e

PROJECT_ID="of-scheduler-proj"
BUCKET_NAME="eros-historical-data-import"
CSV_FILE="/Users/kylemerriman/Desktop/EROS-SYSTEM-SOURCE-OF-TRUTH/Claude.EROS/CORE-BQ-DATA/MASS.MESSAGE.STATs.csv"

echo "🚀 Starting Historical Data Import Process..."

# Step 1: Create GCS bucket (if not exists)
echo "📦 Creating GCS bucket..."
gsutil mb -p $PROJECT_ID gs://$BUCKET_NAME 2>/dev/null || echo "Bucket already exists"

# Step 2: Upload CSV to GCS
echo "⬆️ Uploading CSV to GCS..."
gsutil cp "$CSV_FILE" gs://$BUCKET_NAME/

# Step 3: Create staging table and load data
echo "🗄️ Creating staging table..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
CREATE OR REPLACE TABLE \`$PROJECT_ID.staging.historical_message_staging\` (
  message_text STRING,
  username_raw STRING,
  sending_time STRING,
  price_usd_raw STRING,
  earnings_usd NUMERIC,
  sent INTEGER,
  viewed INTEGER,
  purchased INTEGER,
  view_ratio FLOAT64,
  sent_buy_ratio FLOAT64,
  viewed_buy_ratio FLOAT64,
  message_type STRING,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);"

# Step 4: Load CSV data into staging table
echo "📥 Loading CSV data..."
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_jagged_rows \
  --allow_quoted_newlines \
  --field_delimiter="," \
  --project_id=$PROJECT_ID \
  $PROJECT_ID:staging.historical_message_staging \
  gs://$BUCKET_NAME/MASS.MESSAGE.STATs.csv \
  message_text:STRING,username_raw:STRING,sending_time:STRING,price_usd_raw:STRING,earnings_usd:NUMERIC,sent:INTEGER,viewed:INTEGER,purchased:INTEGER,view_ratio:FLOAT64,sent_buy_ratio:FLOAT64,viewed_buy_ratio:FLOAT64,message_type:STRING

# Step 5: Create filtered view and run analysis
echo "🔍 Creating filtered view for active creators only..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID < /Users/kylemerriman/Desktop/EROS-SYSTEM-SOURCE-OF-TRUTH/historical_data_ingestion.sql

# Step 6: Show preview results
echo "📊 Preview Results:"
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
SELECT 
  'FILTERED HISTORICAL DATA' AS summary,
  COUNT(*) AS total_records,
  COUNT(DISTINCT username_normalized) AS active_creators_found,
  MIN(sending_ts) AS earliest_date,
  MAX(sending_ts) AS latest_date,
  ROUND(SUM(earnings_usd), 2) AS total_revenue
FROM \`$PROJECT_ID.staging.v_historical_filtered\`;"

echo "✅ Historical data staging complete!"
echo "📋 Next step: Review the filtered data and confirm integration to message_facts table."