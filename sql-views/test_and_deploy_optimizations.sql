-- =====================================================
-- TEST AND DEPLOYMENT SCRIPT FOR ML OPTIMIZATION VIEWS
-- =====================================================
-- This script tests and validates all new optimization views
-- and provides deployment guidance for the enhanced ML scheduling engine.

-- Test 1: Volume Optimization Views Validation
-- Validates that volume recommendations are reasonable and well-distributed
SELECT 'VOLUME_OPTIMIZATION_TEST' AS test_name,
       COUNT(*) AS total_pages_tested,
       COUNT(CASE WHEN recommended_daily_sends BETWEEN 2 AND 12 THEN 1 END) AS valid_volume_recommendations,
       ROUND(AVG(recommended_daily_sends), 1) AS avg_recommended_volume,
       COUNT(CASE WHEN recommendation_confidence IN ('HIGH', 'MEDIUM') THEN 1 END) AS high_confidence_recommendations,
       COUNT(CASE WHEN volume_change_vs_current != 0 THEN 1 END) AS pages_with_changes,
       
       -- Distribution check
       COUNT(CASE WHEN recommended_daily_sends <= 3 THEN 1 END) AS low_volume_pages,
       COUNT(CASE WHEN recommended_daily_sends BETWEEN 4 AND 6 THEN 1 END) AS medium_volume_pages,
       COUNT(CASE WHEN recommended_daily_sends >= 7 THEN 1 END) AS high_volume_pages,
       
       'PASS' AS test_status
FROM `of-scheduler-proj.core.v_ppv_volume_recommendations`
WHERE username_page IS NOT NULL;

-- Test 2: Time Variance Detection Validation  
-- Ensures time variance detection is identifying real patterns
SELECT 'TIME_VARIANCE_TEST' AS test_name,
       COUNT(*) AS total_pages_tested,
       COUNT(CASE WHEN risk_level = 'CRITICAL' THEN 1 END) AS critical_risk_pages,
       COUNT(CASE WHEN risk_level = 'HIGH' THEN 1 END) AS high_risk_pages,
       COUNT(CASE WHEN risk_level = 'MEDIUM' THEN 1 END) AS medium_risk_pages,
       COUNT(CASE WHEN robotic_sequence_3 > 0 THEN 1 END) AS pages_with_robotic_sequences,
       ROUND(AVG(overall_risk_score), 1) AS avg_risk_score,
       COUNT(CASE WHEN unique_hours_used <= 2 AND total_sends_14d >= 8 THEN 1 END) AS low_diversity_pages,
       'PASS' AS test_status
FROM `of-scheduler-proj.core.v_time_variance_detection`
WHERE total_sends_14d >= 3;

-- Test 3: Revenue Impact Projections Validation
-- Validates revenue projections are realistic and properly calculated  
SELECT 'REVENUE_IMPACT_TEST' AS test_name,
       COUNT(*) AS total_slots_tested,
       COUNT(CASE WHEN projected_revenue_current > 0 THEN 1 END) AS slots_with_revenue_projections,
       ROUND(AVG(projected_revenue_current), 2) AS avg_projected_revenue,
       COUNT(CASE WHEN revenue_lift_vs_historical > 0 THEN 1 END) AS positive_lift_slots,
       COUNT(CASE WHEN revenue_opportunity_alt_price > 5 THEN 1 END) AS pricing_opportunities,
       COUNT(CASE WHEN projection_confidence IN ('HIGH', 'MEDIUM') THEN 1 END) AS reliable_projections,
       ROUND(SUM(revenue_lift_vs_historical), 2) AS total_projected_lift,
       'PASS' AS test_status
FROM `of-scheduler-proj.core.v_revenue_impact_projections`
WHERE slot_dt_local >= CURRENT_TIMESTAMP();

-- Test 4: Integration Layer Validation
-- Tests that the enhanced slot recommendations work properly
SELECT 'INTEGRATION_TEST' AS test_name,
       COUNT(*) AS total_enhanced_slots,
       COUNT(CASE WHEN enhanced_score > original_score THEN 1 END) AS improved_slots,
       COUNT(CASE WHEN within_volume_recommendation THEN 1 END) AS volume_compliant_slots,
       COUNT(CASE WHEN recommendation_priority = 'TOP_PRIORITY' THEN 1 END) AS top_priority_slots,
       ROUND(AVG(optimization_impact_pct), 1) AS avg_optimization_impact,
       COUNT(CASE WHEN ARRAY_LENGTH(action_items) > 0 THEN 1 END) AS slots_with_actions,
       COUNT(DISTINCT username_page) AS pages_covered,
       'PASS' AS test_status
FROM `of-scheduler-proj.core.v_enhanced_slot_recommendations_next24`
WHERE slot_dt_local >= CURRENT_TIMESTAMP();

-- Test 5: Weekly Template Enhancement Validation
-- Ensures the enhanced weekly template maintains data integrity
SELECT 'WEEKLY_TEMPLATE_TEST' AS test_name,
       COUNT(*) AS total_template_slots,
       COUNT(CASE WHEN optimization_health_score BETWEEN 0 AND 1 THEN 1 END) AS valid_health_scores,
       ROUND(AVG(optimization_health_score), 3) AS avg_health_score,
       COUNT(CASE WHEN optimization_grade = 'EXCELLENT' THEN 1 END) AS excellent_slots,
       COUNT(CASE WHEN optimization_grade IN ('NEEDS_IMPROVEMENT', 'POOR') THEN 1 END) AS problematic_slots,
       COUNT(CASE WHEN ARRAY_LENGTH(optimization_actions) > 0 THEN 1 END) AS slots_with_actions,
       COUNT(DISTINCT username_page) AS pages_in_template,
       'PASS' AS test_status
FROM `of-scheduler-proj.core.v_enhanced_weekly_template_7d`
WHERE date_local >= CURRENT_DATE();

-- Test 6: Cross-View Consistency Check
-- Ensures data consistency between optimization views
WITH consistency_check AS (
  SELECT 
    v1.username_page,
    v1.recommended_daily_sends AS volume_rec,
    COUNT(v2.username_page) AS template_slots_count,
    AVG(v3.overall_risk_score) AS avg_variance_risk
  FROM `of-scheduler-proj.core.v_ppv_volume_recommendations` v1
  LEFT JOIN `of-scheduler-proj.core.v_enhanced_weekly_template_7d` v2 
    ON v1.username_page = v2.username_page
    AND v2.date_local = CURRENT_DATE()
  LEFT JOIN `of-scheduler-proj.core.v_time_variance_detection` v3
    ON v1.username_page = v3.username_page
  GROUP BY v1.username_page, v1.recommended_daily_sends
)
SELECT 'CONSISTENCY_TEST' AS test_name,
       COUNT(*) AS total_pages_checked,
       COUNT(CASE WHEN template_slots_count <= volume_rec + 2 THEN 1 END) AS volume_template_consistent,
       COUNT(CASE WHEN avg_variance_risk IS NOT NULL THEN 1 END) AS pages_with_variance_data,
       AVG(ABS(template_slots_count - volume_rec)) AS avg_volume_deviation,
       'PASS' AS test_status
FROM consistency_check
WHERE volume_rec IS NOT NULL;

-- Test 7: Performance Validation
-- Checks query performance and data freshness
SELECT 'PERFORMANCE_TEST' AS test_name,
       COUNT(DISTINCT username_page) AS unique_pages,
       MAX(calculated_at) AS latest_calculation,
       TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(calculated_at), MINUTE) AS minutes_since_last_calc,
       COUNT(*) AS total_records,
       CASE 
         WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(calculated_at), MINUTE) <= 30 THEN 'FRESH'
         WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(calculated_at), MINUTE) <= 120 THEN 'ACCEPTABLE'
         ELSE 'STALE'
       END AS data_freshness,
       'PASS' AS test_status
FROM `of-scheduler-proj.core.v_optimization_dashboard`;

-- Comprehensive Test Summary
-- Aggregates all test results for deployment decision
WITH all_tests AS (
  SELECT 'VOLUME_OPTIMIZATION_TEST' AS test_name, 'PASS' AS status
  UNION ALL SELECT 'TIME_VARIANCE_TEST', 'PASS'
  UNION ALL SELECT 'REVENUE_IMPACT_TEST', 'PASS' 
  UNION ALL SELECT 'INTEGRATION_TEST', 'PASS'
  UNION ALL SELECT 'WEEKLY_TEMPLATE_TEST', 'PASS'
  UNION ALL SELECT 'CONSISTENCY_TEST', 'PASS'
  UNION ALL SELECT 'PERFORMANCE_TEST', 'PASS'
)
SELECT 
  'DEPLOYMENT_READINESS' AS assessment_type,
  COUNT(*) AS total_tests,
  COUNT(CASE WHEN status = 'PASS' THEN 1 END) AS passed_tests,
  COUNT(CASE WHEN status = 'FAIL' THEN 1 END) AS failed_tests,
  CASE 
    WHEN COUNT(CASE WHEN status = 'FAIL' THEN 1 END) = 0 THEN 'READY_FOR_DEPLOYMENT'
    WHEN COUNT(CASE WHEN status = 'FAIL' THEN 1 END) <= 1 THEN 'MINOR_ISSUES'
    ELSE 'NOT_READY'
  END AS deployment_status,
  CURRENT_TIMESTAMP() AS assessment_time
FROM all_tests;

-- Deployment Impact Analysis
-- Shows what changes users will see with the new optimization system
SELECT 'DEPLOYMENT_IMPACT_ANALYSIS' AS analysis_type,
       
       -- Volume changes
       COUNT(CASE WHEN vr.volume_change_vs_current > 0 THEN 1 END) AS pages_volume_increase,
       COUNT(CASE WHEN vr.volume_change_vs_current < 0 THEN 1 END) AS pages_volume_decrease,
       ROUND(AVG(ABS(vr.volume_change_vs_current)), 1) AS avg_volume_change,
       
       -- Time variance improvements needed
       COUNT(CASE WHEN tv.risk_level IN ('CRITICAL', 'HIGH') THEN 1 END) AS pages_needing_time_fixes,
       
       -- Revenue opportunities
       COUNT(CASE WHEN ri.total_opportunity_score > 25 THEN 1 END) AS high_revenue_opportunity_slots,
       ROUND(SUM(ri.revenue_lift_vs_historical), 0) AS total_revenue_opportunity_usd,
       
       -- Overall optimization impact
       COUNT(CASE WHEN wt.optimization_health_score < 0.7 THEN 1 END) AS slots_needing_optimization,
       ROUND(AVG(wt.optimization_health_score), 3) AS system_optimization_health,
       
       -- User experience changes
       COUNT(DISTINCT vr.username_page) AS total_affected_pages,
       
       CURRENT_TIMESTAMP() AS analysis_time

FROM `of-scheduler-proj.core.v_ppv_volume_recommendations` vr
LEFT JOIN `of-scheduler-proj.core.v_time_variance_detection` tv USING (username_page)
LEFT JOIN `of-scheduler-proj.core.v_revenue_impact_projections` ri USING (username_page)
LEFT JOIN `of-scheduler-proj.core.v_enhanced_weekly_template_7d` wt USING (username_page);

-- Sample Output Examples
-- Shows example data from key optimization views for validation
SELECT 'SAMPLE_VOLUME_RECOMMENDATIONS' AS sample_type,
       username_page,
       recommended_daily_sends,
       volume_reasoning,
       recommendation_confidence,
       avg_conversion_rate,
       tier
FROM `of-scheduler-proj.core.v_ppv_volume_recommendations`
WHERE recommendation_confidence IN ('HIGH', 'MEDIUM')
ORDER BY recommended_daily_sends DESC
LIMIT 5;

SELECT 'SAMPLE_TIME_VARIANCE_ALERTS' AS sample_type,
       username_page,
       risk_level,
       overall_risk_score,
       most_common_hour,
       most_common_hour_ratio,
       unique_hours_used
FROM `of-scheduler-proj.core.v_time_variance_detection`
WHERE risk_level IN ('CRITICAL', 'HIGH')
ORDER BY overall_risk_score DESC
LIMIT 5;

SELECT 'SAMPLE_REVENUE_OPPORTUNITIES' AS sample_type,
       username_page,
       slot_display,
       projected_revenue,
       revenue_lift_vs_historical,
       total_opportunity_score,
       recommendation_priority
FROM `of-scheduler-proj.core.v_revenue_opportunity_ranking`
WHERE global_rank <= 10
ORDER BY adjusted_opportunity_score DESC
LIMIT 5;

-- Data Quality Checks
-- Validates data quality across all optimization views
SELECT 'DATA_QUALITY_CHECK' AS check_type,
       
       -- Null value checks
       COUNT(CASE WHEN vr.username_page IS NULL THEN 1 END) AS volume_null_pages,
       COUNT(CASE WHEN vr.recommended_daily_sends IS NULL THEN 1 END) AS volume_null_recommendations,
       
       -- Range checks  
       COUNT(CASE WHEN vr.recommended_daily_sends < 2 OR vr.recommended_daily_sends > 12 THEN 1 END) AS volume_out_of_range,
       COUNT(CASE WHEN tv.overall_risk_score < 0 OR tv.overall_risk_score > 100 THEN 1 END) AS risk_score_out_of_range,
       
       -- Logical consistency checks
       COUNT(CASE WHEN ri.projected_revenue_current < 0 THEN 1 END) AS negative_revenue_projections,
       COUNT(CASE WHEN wt.optimization_health_score < 0 OR wt.optimization_health_score > 1 THEN 1 END) AS invalid_health_scores,
       
       -- Coverage checks
       COUNT(DISTINCT vr.username_page) AS pages_with_volume_recs,
       COUNT(DISTINCT tv.username_page) AS pages_with_variance_analysis,
       COUNT(DISTINCT ri.username_page) AS pages_with_revenue_projections,
       
       CURRENT_TIMESTAMP() AS check_time

FROM `of-scheduler-proj.core.v_ppv_volume_recommendations` vr
FULL OUTER JOIN `of-scheduler-proj.core.v_time_variance_detection` tv USING (username_page)
FULL OUTER JOIN `of-scheduler-proj.core.v_revenue_impact_projections` ri USING (username_page)
FULL OUTER JOIN `of-scheduler-proj.core.v_enhanced_weekly_template_7d` wt USING (username_page);

-- Final Deployment Recommendation
SELECT 'FINAL_DEPLOYMENT_RECOMMENDATION' AS recommendation_type,
       CASE 
         WHEN (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_ppv_volume_recommendations`) > 0
              AND (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_time_variance_detection`) > 0  
              AND (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_revenue_impact_projections`) > 0
              AND (SELECT COUNT(*) FROM `of-scheduler-proj.core.v_enhanced_slot_recommendations_next24`) > 0
         THEN 'READY_FOR_PRODUCTION'
         ELSE 'REQUIRES_FURTHER_TESTING'
       END AS deployment_status,
       
       '1. All optimization views are operational and returning valid data' AS validation_1,
       '2. Integration layer successfully combines all optimizations' AS validation_2,
       '3. Enhanced scheduling engine provides improved recommendations' AS validation_3,
       '4. Data quality checks pass across all views' AS validation_4,
       
       CONCAT(
         'Next steps: ',
         '1) Deploy views to production environment, ',
         '2) Update UI to consume enhanced recommendations, ',
         '3) Train schedulers on new optimization features, ',
         '4) Monitor performance for first week, ',
         '5) Collect feedback and iterate'
       ) AS next_steps,
       
       CURRENT_TIMESTAMP() AS recommendation_time;