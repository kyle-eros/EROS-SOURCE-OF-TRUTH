#!/bin/bash

# =========================================
# Deploy Monitoring Dashboard Views
# =========================================

set -e

PROJECT_ID="of-scheduler-proj"
SCRIPT_DIR="$(dirname "$0")"

echo "=========================================
DEPLOYING MONITORING DASHBOARD VIEWS
========================================="

# Create ops_monitor dataset if it doesn't exist
echo "Creating ops_monitor dataset..."
bq mk --dataset --location=US --project_id=$PROJECT_ID ops_monitor 2>/dev/null || echo "Dataset already exists"

# Create pipeline_runs table for tracking
echo "Creating pipeline_runs tracking table..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<EOF
CREATE TABLE IF NOT EXISTS \`$PROJECT_ID.ops_monitor.pipeline_runs\` (
  run_timestamp TIMESTAMP,
  pipeline_name STRING,
  status STRING,
  records_processed INT64,
  duration_seconds INT64,
  trigger_type STRING
)
EOF

echo ""
echo "Deploying monitoring views..."
echo ""

# Split queries and deploy individually
echo "1. System Health Overview..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<'EOF'
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_monitor.dashboard_system_health` AS
SELECT
  CURRENT_TIMESTAMP() AS check_time,
  
  -- Overall health score (0-100)
  CAST(
    (CASE WHEN freshness.feature_store_age_hours < 26 THEN 25 ELSE 0 END) +
    (CASE WHEN quality.eligible_rate > 0.3 THEN 25 ELSE 15 END) +
    (CASE WHEN volume.daily_sends > 100 THEN 25 ELSE 15 END) +
    (CASE WHEN performance.avg_confidence > 0.05 THEN 25 ELSE 15 END)
  AS INT64) AS health_score,
  
  -- Component statuses
  freshness.feature_store_age_hours,
  quality.eligible_rate,
  volume.daily_sends,
  performance.avg_confidence,
  performance.avg_rps,
  
  -- Alert status
  CASE
    WHEN freshness.feature_store_age_hours > 48 THEN 'CRITICAL - Stale Data'
    WHEN quality.eligible_rate < 0.1 THEN 'WARNING - Low Eligibility'
    WHEN volume.daily_sends < 10 THEN 'WARNING - Low Volume'
    ELSE 'HEALTHY'
  END AS alert_status
  
FROM (
  -- Data freshness
  SELECT 
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(computed_at), HOUR) AS feature_store_age_hours
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
) freshness
CROSS JOIN (
  -- Quality metrics
  SELECT
    AVG(CASE WHEN cooldown_features.is_eligible THEN 1.0 ELSE 0.0 END) AS eligible_rate
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date = CURRENT_DATE()
) quality
CROSS JOIN (
  -- Volume metrics
  SELECT
    COUNT(*) AS daily_sends
  FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
  WHERE send_date = CURRENT_DATE()
) volume
CROSS JOIN (
  -- Performance metrics
  SELECT
    AVG(performance_features.confidence_score) AS avg_confidence,
    AVG(performance_features.rps_smoothed) AS avg_rps
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date = CURRENT_DATE()
) performance
EOF

echo "2. ML Performance Tracking..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<'EOF'
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_monitor.dashboard_ml_performance` AS
WITH daily_metrics AS (
  SELECT
    computed_date,
    COUNT(*) AS features_computed,
    AVG(performance_features.rps_smoothed) AS avg_rps,
    AVG(performance_features.confidence_score) AS avg_confidence,
    AVG(CASE WHEN cooldown_features.is_eligible THEN 1.0 ELSE 0.0 END) AS eligibility_rate,
    APPROX_QUANTILES(composite_scores.base_score, 100)[OFFSET(50)] AS median_score,
    COUNT(DISTINCT username_page) AS active_pages
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY computed_date
)
SELECT
  computed_date,
  features_computed,
  ROUND(avg_rps, 4) AS avg_rps,
  ROUND(avg_confidence, 3) AS avg_confidence,
  ROUND(eligibility_rate * 100, 1) AS eligibility_rate_pct,
  ROUND(median_score, 3) AS median_score,
  active_pages,
  
  -- Trend indicators
  avg_rps - LAG(avg_rps) OVER (ORDER BY computed_date) AS rps_change,
  eligibility_rate - LAG(eligibility_rate) OVER (ORDER BY computed_date) AS eligibility_change
  
FROM daily_metrics
ORDER BY computed_date DESC
EOF

echo "3. Caption Performance Leaderboard..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<'EOF'
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_monitor.dashboard_top_captions` AS
SELECT
  caption_id,
  username_page,
  caption_category,
  
  -- Performance metrics
  ROUND(performance_features.rps_smoothed, 4) AS rps,
  ROUND(performance_features.confidence_score, 3) AS confidence,
  performance_features.sends_30d AS recent_sends,
  
  -- Ranking
  RANK() OVER (ORDER BY performance_features.rps_smoothed DESC) AS rps_rank,
  RANK() OVER (ORDER BY performance_features.confidence_score DESC) AS confidence_rank,
  RANK() OVER (ORDER BY composite_scores.base_score DESC) AS overall_rank,
  
  -- Status
  CASE
    WHEN cooldown_features.is_eligible THEN 'Eligible'
    WHEN cooldown_features.fatigue_score > 0.8 THEN 'Fatigued'
    ELSE 'Cooling Down'
  END AS status,
  
  temporal_features.hours_since_use AS hours_since_last_use
  
FROM `of-scheduler-proj.layer_05_ml.feature_store`
WHERE computed_date = CURRENT_DATE()
ORDER BY overall_rank
LIMIT 100
EOF

echo "4. Exploration vs Exploitation Balance..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<'EOF'
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_monitor.dashboard_exploration_balance` AS
WITH exploration_metrics AS (
  SELECT
    username_page,
    COUNT(*) AS total_captions,
    
    -- Categorize by exploration status
    SUM(CASE WHEN performance_features.sends_30d < 10 THEN 1 ELSE 0 END) AS new_captions,
    SUM(CASE WHEN performance_features.sends_30d BETWEEN 10 AND 50 THEN 1 ELSE 0 END) AS exploring_captions,
    SUM(CASE WHEN performance_features.sends_30d > 50 THEN 1 ELSE 0 END) AS exploiting_captions,
    
    -- Average scores by category
    AVG(CASE WHEN performance_features.sends_30d < 10 THEN exploration_features.novelty_bonus ELSE NULL END) AS avg_novelty_bonus,
    AVG(exploration_features.ucb_bonus) AS avg_ucb_bonus
    
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date = CURRENT_DATE()
  GROUP BY username_page
)
SELECT
  username_page,
  total_captions,
  new_captions,
  exploring_captions,
  exploiting_captions,
  
  -- Percentages
  ROUND(100.0 * new_captions / NULLIF(total_captions, 0), 1) AS new_pct,
  ROUND(100.0 * exploring_captions / NULLIF(total_captions, 0), 1) AS exploring_pct,
  ROUND(100.0 * exploiting_captions / NULLIF(total_captions, 0), 1) AS exploiting_pct,
  
  -- Exploration health
  CASE
    WHEN new_captions > 0.3 * total_captions THEN 'High Exploration'
    WHEN new_captions > 0.1 * total_captions THEN 'Balanced'
    ELSE 'Low Exploration'
  END AS exploration_status,
  
  ROUND(avg_novelty_bonus, 3) AS avg_novelty_bonus,
  ROUND(avg_ucb_bonus, 3) AS avg_ucb_bonus
  
FROM exploration_metrics
ORDER BY total_captions DESC
EOF

echo "5. Hourly Performance Patterns..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<'EOF'
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_monitor.dashboard_hourly_patterns` AS
SELECT
  temporal_features.best_hour AS hour_utc,
  COUNT(*) AS caption_count,
  AVG(temporal_features.best_hour_rps) AS avg_best_hour_rps,
  AVG(performance_features.rps_smoothed) AS avg_overall_rps,
  
  -- Performance lift at best hour
  ROUND(100.0 * (AVG(temporal_features.best_hour_rps) - AVG(performance_features.rps_smoothed)) / 
        NULLIF(AVG(performance_features.rps_smoothed), 0), 1) AS performance_lift_pct
  
FROM `of-scheduler-proj.layer_05_ml.feature_store`
WHERE computed_date = CURRENT_DATE()
GROUP BY hour_utc
ORDER BY hour_utc
EOF

echo "6. Data Quality Monitoring..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<'EOF'
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_monitor.dashboard_data_quality` AS
SELECT
  'Feature Store' AS dataset,
  COUNT(*) AS total_records,
  
  -- Null checks
  SUM(CASE WHEN caption_id IS NULL THEN 1 ELSE 0 END) AS null_caption_ids,
  SUM(CASE WHEN performance_features.rps_smoothed IS NULL THEN 1 ELSE 0 END) AS null_rps,
  
  -- Range checks
  SUM(CASE WHEN performance_features.confidence_score < 0 OR performance_features.confidence_score > 1 THEN 1 ELSE 0 END) AS confidence_out_of_range,
  SUM(CASE WHEN cooldown_features.fatigue_score < 0 OR cooldown_features.fatigue_score > 1 THEN 1 ELSE 0 END) AS fatigue_out_of_range,
  
  -- Consistency checks
  SUM(CASE WHEN performance_features.sends_30d = 0 AND performance_features.rps_30d > 0 THEN 1 ELSE 0 END) AS inconsistent_metrics,
  
  -- Overall quality score
  ROUND(100.0 * (1 - (
    SUM(CASE WHEN caption_id IS NULL THEN 1 ELSE 0 END) +
    SUM(CASE WHEN performance_features.confidence_score < 0 OR performance_features.confidence_score > 1 THEN 1 ELSE 0 END)
  ) / NULLIF(COUNT(*), 0)), 1) AS quality_score_pct
  
FROM `of-scheduler-proj.layer_05_ml.feature_store`
WHERE computed_date = CURRENT_DATE()

UNION ALL

SELECT
  'Fact Table' AS dataset,
  COUNT(*) AS total_records,
  SUM(CASE WHEN caption_key = 'UNKNOWN' THEN 1 ELSE 0 END) AS null_caption_ids,
  SUM(CASE WHEN revenue_per_send IS NULL THEN 1 ELSE 0 END) AS null_rps,
  SUM(CASE WHEN messages_purchased > messages_sent THEN 1 ELSE 0 END) AS confidence_out_of_range,
  0 AS fatigue_out_of_range,
  SUM(CASE WHEN quality_flag != 'valid' THEN 1 ELSE 0 END) AS inconsistent_metrics,
  ROUND(100.0 * SUM(CASE WHEN quality_flag = 'valid' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS quality_score_pct
FROM `of-scheduler-proj.layer_03_foundation.fact_message_send`
WHERE send_date = CURRENT_DATE()
EOF

echo "7. Alerts and Anomalies..."
bq query --use_legacy_sql=false --project_id=$PROJECT_ID <<'EOF'
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_monitor.dashboard_alerts` AS
WITH current_metrics AS (
  SELECT
    COUNT(*) AS feature_count,
    AVG(performance_features.rps_smoothed) AS avg_rps,
    AVG(performance_features.confidence_score) AS avg_confidence
  FROM `of-scheduler-proj.layer_05_ml.feature_store`
  WHERE computed_date = CURRENT_DATE()
),
historical_metrics AS (
  SELECT
    AVG(cnt) AS avg_feature_count,
    AVG(rps) AS historical_avg_rps,
    STDDEV(rps) AS historical_stddev_rps
  FROM (
    SELECT
      computed_date,
      COUNT(*) AS cnt,
      AVG(performance_features.rps_smoothed) AS rps
    FROM `of-scheduler-proj.layer_05_ml.feature_store`
    WHERE computed_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY computed_date
  )
)
SELECT
  CURRENT_TIMESTAMP() AS alert_time,
  
  -- Generate alerts
  ARRAY_CONCAT(
    -- Data freshness alert
    IF(
      (SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(computed_at), HOUR) 
       FROM `of-scheduler-proj.layer_05_ml.feature_store`) > 26,
      [STRUCT('CRITICAL' AS severity, 'Data Freshness' AS alert_type, 'Feature store not updated in >26 hours' AS message)],
      []
    ),
    
    -- Volume anomaly alert
    IF(
      ABS(cm.feature_count - hm.avg_feature_count) > 3 * SQRT(hm.avg_feature_count),
      [STRUCT('WARNING' AS severity, 'Volume Anomaly' AS alert_type, 
              CONCAT('Feature count (', CAST(cm.feature_count AS STRING), ') deviates from normal') AS message)],
      []
    ),
    
    -- Performance degradation alert
    IF(
      cm.avg_rps < hm.historical_avg_rps - 2 * hm.historical_stddev_rps,
      [STRUCT('WARNING' AS severity, 'Performance Degradation' AS alert_type,
              CONCAT('Average RPS (', CAST(ROUND(cm.avg_rps, 4) AS STRING), ') below normal range') AS message)],
      []
    ),
    
    -- Low confidence alert
    IF(
      cm.avg_confidence < 0.05,
      [STRUCT('INFO' AS severity, 'Low Confidence' AS alert_type,
              'Average confidence score below threshold' AS message)],
      []
    )
  ) AS alerts
  
FROM current_metrics cm
CROSS JOIN historical_metrics hm
EOF

echo ""
echo "========================================="
echo "✓ Monitoring dashboard views deployed!"
echo "========================================="
echo ""
echo "Views created in: $PROJECT_ID.ops_monitor"
echo ""
echo "Available dashboards:"
echo "  - dashboard_system_health"
echo "  - dashboard_ml_performance"
echo "  - dashboard_top_captions"
echo "  - dashboard_exploration_balance"
echo "  - dashboard_hourly_patterns"
echo "  - dashboard_data_quality"
echo "  - dashboard_alerts"
echo ""
echo "You can now connect these views to:"
echo "  - Looker Studio"
echo "  - Tableau"
echo "  - Custom dashboards"
echo "  - Apps Script monitoring"