# CUTOVER PLAN - ML Scheduling v.Next

## Overview
This document provides the step-by-step cutover plan from the current broken ML scheduling system to the new v.Next implementation.

## Current State Issues
- ❌ **Missing caption_id**: All slots have empty caption IDs
- ❌ **No caption_text**: Apps Script cannot display captions
- ❌ **Price ladder duplication**: Same caption hash repeated 5x per slot
- ❌ **Score anomalies**: Range 0-1498 (not normalized)
- ❌ **No cooldown enforcement**: Missing rotation logic
- ❌ **No exploration**: Cold-start captions never selected

## Target State (v.Next)
- ✅ **Complete caption data**: Both caption_id and caption_text provided
- ✅ **UCB exploration**: Automatic cold-start handling
- ✅ **Configurable weights**: All parameters in ops tables
- ✅ **Quality monitoring**: Automated QA with rollback triggers
- ✅ **Cost controls**: Partition pruning and byte limits

## Pre-Cutover Checklist

### 1. Deploy Config Tables (Day -7)
```bash
# Create configuration tables with seed data
bq query --use_legacy_sql=false < bigquery/ops/ml_config_tables.sql

# Verify config tables
bq ls -n 1000 of-scheduler-proj:ops
bq head -n 5 of-scheduler-proj:ops.ml_ranking_weights_v1
bq head -n 5 of-scheduler-proj:ops.explore_exploit_config_v1
```

### 2. Deploy New Views (Day -5)
```bash
# Deploy feature engineering
bq query --use_legacy_sql=false < bigquery/mart/caption_features_vNext.sql

# Deploy ranker
bq query --use_legacy_sql=false < bigquery/mart/caption_ranker_vNext.sql

# Deploy sheets export (CRITICAL)
bq query --use_legacy_sql=false < bigquery/mart/sheets_schedule_export_vNext.sql

# Deploy QA views
bq query --use_legacy_sql=false < bigquery/qa/validation_suite_vNext.sql
```

### 3. Run Test Suite (Day -3)
```bash
cd bigquery/tests
chmod +x test_runner.sh

# Run all tests
./test_runner.sh all

# Verify acceptance criteria
./test_runner.sh acceptance
```

### 4. Shadow Testing (Day -2 to -1)
```sql
-- Compare old vs new output
WITH old_data AS (
  SELECT * FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`
  WHERE schedule_date = CURRENT_DATE()
),
new_data AS (
  SELECT * FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`
  WHERE schedule_date = CURRENT_DATE()
)
SELECT
  COUNT(DISTINCT o.username_page) AS old_pages,
  COUNT(DISTINCT n.username_page) AS new_pages,
  COUNT(DISTINCT n.caption_id) AS new_caption_ids,
  AVG(n.score_final) AS avg_score_new
FROM old_data o
FULL OUTER JOIN new_data n USING (username_page);
```

## Cutover Steps (Day 0)

### Phase 1: Update Apps Script (9:00 AM)
```javascript
// In Main.js, update line 182 to use new view:
// OLD: FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`
// NEW: FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`

// Update the query in getWeeklyRecommendations():
const sql = [
  'SELECT * FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`',
  'WHERE schedule_date BETWEEN @start_date AND @end_date',
  'ORDER BY schedule_date, username_std, page_type, slot_rank, ladder_position'
].join('\n');
```

### Phase 2: Create View Alias (9:30 AM)
```sql
-- Create backward-compatible alias
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_weekly_template_7d_pages_final_backup` AS
SELECT * FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`;

-- Point old view to new implementation
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_weekly_template_7d_pages_final` AS
SELECT 
  username_page,
  Model AS username_std,
  Page AS page_type,
  Date AS schedule_date,
  CAST(SUBSTR(`Rec Time`, 1, 2) AS INT64) AS hod_local,
  `Rec Price` AS price_usd,
  `Tracking Hash` AS tracking_hash,
  CAST(SUBSTR(Slot, -1) AS INT64) - 1 AS slot_rank,
  `Rec Caption ID` AS caption_id,
  `Caption Preview` AS caption_text
FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`;
```

### Phase 3: Refresh Sheets (10:00 AM)
```javascript
// In Google Sheets, run:
refreshWeekPlan();

// Verify output has:
// - Non-empty caption IDs
// - Caption preview text
// - Normalized scores (0-100)
```

### Phase 4: Monitor QA (10:30 AM)
```sql
-- Check health metrics
SELECT * FROM `of-scheduler-proj.qa.daily_health_vNext`;

-- Monitor for rollback triggers
SELECT 
  metric_name,
  metric_value,
  status,
  trigger_rollback
FROM `of-scheduler-proj.qa.daily_health_vNext`
WHERE trigger_rollback = TRUE;
```

## Rollback Plan

### Immediate Rollback (if needed)
```sql
-- Revert view alias
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_weekly_template_7d_pages_final` AS
SELECT * FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final_backup`;
```

```javascript
// Revert Apps Script
// Change back to original view name in Main.js line 182
```

### Rollback Triggers
Automatic rollback if ANY of these occur:
- Caption fill rate < 99.9%
- Cooldown violation rate > 1%
- Fallback usage > 3%
- Diversity ratio < 85%
- Bytes billed > 10GB per build

## Post-Cutover Validation

### Hour 1: Data Quality
```bash
./test_runner.sh qa
```

### Hour 4: Performance Metrics
```sql
SELECT
  AVG(expected_rps) AS avg_expected_rps,
  AVG(expected_conversion) AS avg_expected_conversion,
  AVG(timing_quality) AS avg_timing_quality
FROM `of-scheduler-proj.mart.sheets_schedule_export_vNext`
WHERE schedule_date = CURRENT_DATE();
```

### Day 1: Full Acceptance
```bash
./test_runner.sh acceptance
```

### Day 7: Backtest Validation
```bash
./test_runner.sh backtest
```

## Monitoring Dashboard
Create Looker Studio dashboard pointing to:
- `qa.daily_health_vNext`
- `qa.caption_diversity_check_vNext`
- `qa.cooldown_violations_vNext`
- `qa.data_completeness_vNext`

## Support Contacts
- ML Engineering: [Your Team]
- Data Platform: [Platform Team]
- On-call: [PagerDuty]

## Success Criteria
✅ Week Plan shows caption IDs and text
✅ Scores normalized to 0-100
✅ Diversity > 85%
✅ Cooldown violations < 1%
✅ Fallback usage < 3%
✅ Daily cost < $10