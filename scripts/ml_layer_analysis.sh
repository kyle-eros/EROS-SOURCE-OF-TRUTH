#!/bin/bash

echo "=============================================="
echo "COMPLETE ML LAYER ARCHITECTURE ANALYSIS"
echo "=============================================="
echo ""

echo "1. CONFIGURATION LAYER (ops_config)"
echo "--------------------------------------------"
bq query --use_legacy_sql=false --format=pretty << 'SQL'
SELECT 'Feature Flags' as config_type, flag_name, is_enabled
FROM `of-scheduler-proj.ops_config.feature_flags`
ORDER BY flag_name;
SQL

echo ""
bq query --use_legacy_sql=false --format=pretty << 'SQL'
SELECT 'Tier Configurations' as config_type, 
       tier, anchors_per_day, supports_per_day, min_spacing_minutes, jitter_minutes
FROM `of-scheduler-proj.ops_config.tier_slot_packs`
ORDER BY tier;
SQL

echo ""
bq query --use_legacy_sql=false --format=pretty << 'SQL'
SELECT 'ML Bandit Parameters' as config_type,
       tier, alpha, beta, epsilon, ucb_c, base_cooldown_hours
FROM `of-scheduler-proj.ops_config.ml_params_bandit`
ORDER BY tier;
SQL

echo ""
echo "2. DATA FLOW ARCHITECTURE"
echo "--------------------------------------------"
bq query --use_legacy_sql=false << 'SQL'
WITH layer_info AS (
  SELECT 
    'Layer 02: Staging' AS layer,
    'stg_message_events' AS main_table,
    'Raw event data ingestion' AS purpose,
    1 AS order_num
  UNION ALL
  SELECT 
    'Layer 03: Foundation',
    'fact_message_send, dim_creator, dim_caption',
    'Normalized fact/dimension model',
    2
  UNION ALL
  SELECT 
    'Layer 04: Semantic',
    'v_page_dow_hod_profile_90d, v_page_behavior_28d, v_page_intensity_7d',
    'Business metrics and aggregations',
    3
  UNION ALL
  SELECT 
    'Layer 05: ML',
    'feature_store, v_rank_ready, tvf_rank_captions',
    'ML features and ranking algorithms',
    4
  UNION ALL
  SELECT 
    'Layer 07: Export',
    'tvf_weekly_template, schedule_recommendations',
    'API-ready outputs and schedules',
    5
)
SELECT layer, main_table, purpose
FROM layer_info
ORDER BY order_num;
SQL

echo ""
echo "3. FEATURE STORE STRUCTURE"
echo "--------------------------------------------"
bq show --schema --format=prettyjson of-scheduler-proj:layer_05_ml.feature_store | python3 -c "
import json, sys
data = json.load(sys.stdin)
print('Feature Store Schema:')
for field in data:
    if field['type'] == 'RECORD':
        print(f'  - {field[\"name\"]} (STRUCT):')
        if 'fields' in field:
            for subfield in field['fields']:
                print(f'      • {subfield[\"name\"]}: {subfield[\"type\"]}')
    else:
        print(f'  - {field[\"name\"]}: {field[\"type\"]}')
"

echo ""
echo "4. MULTI-ARMED BANDIT ALGORITHMS"
echo "--------------------------------------------"
bq query --use_legacy_sql=false << 'SQL'
SELECT 
  'UCB (Upper Confidence Bound)' AS algorithm,
  'Balances exploitation vs exploration using confidence intervals' AS description,
  'baseline_mean + sqrt(2*ln(total_trials)/item_trials) * c' AS formula
UNION ALL
SELECT 
  'Epsilon-Greedy',
  'Random exploration with probability epsilon, exploit best otherwise',
  'if (random < epsilon) then explore else use best'
UNION ALL
SELECT 
  'Thompson Sampling',
  'Samples from posterior Beta distribution for each arm',
  'sample from Beta(alpha + successes, beta + failures)'
ORDER BY algorithm;
SQL

echo ""
echo "5. SEMANTIC LAYER METRICS"
echo "--------------------------------------------"
bq query --use_legacy_sql=false << 'SQL'
WITH view_columns AS (
  SELECT 
    table_name,
    column_name,
    data_type
  FROM `of-scheduler-proj.layer_04_semantic.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name IN ('v_page_dow_hod_profile_90d', 'v_page_behavior_28d', 'v_page_intensity_7d')
    AND column_name NOT IN ('page_key')
  ORDER BY table_name, ordinal_position
)
SELECT 
  REPLACE(table_name, 'v_', '') as view_name,
  STRING_AGG(column_name, ', ') as key_metrics
FROM view_columns
GROUP BY table_name;
SQL

echo ""
echo "=============================================="
