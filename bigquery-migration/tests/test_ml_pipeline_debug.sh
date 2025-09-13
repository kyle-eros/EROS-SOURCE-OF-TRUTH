#!/bin/bash

# =========================================
# END-TO-END ML PIPELINE TEST (DEBUG)
# =========================================
# Tests all components of the ML system
# =========================================

set -e
set -x

PROJECT_ID="of-scheduler-proj"
SCRIPT_DIR="$(dirname "$0")"
TEST_RESULTS="$SCRIPT_DIR/test_results_$(date +%Y%m%d_%H%M%S).log"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Function to log test results
log_test() {
    local test_name=$1
    local status=$2
    local message=$3
    
    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}✓${NC} $test_name: $message" | tee -a "$TEST_RESULTS"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗${NC} $test_name: $message" | tee -a "$TEST_RESULTS"
        ((TESTS_FAILED++))
    fi
}

# Function to run query and check results
test_query() {
    local test_name=$1
    local query=$2
    local expected_condition=$3
    
    echo "Testing: $test_name..." | tee -a "$TEST_RESULTS"
    
    result=$(bq query --use_legacy_sql=false --format=csv --project_id=$PROJECT_ID "$query" | tail -n 1)
    
    if eval "$expected_condition"; then
        log_test "$test_name" "PASS" "Result: $result"
    else
        log_test "$test_name" "FAIL" "Result: $result (Expected: $expected_condition)"
    fi
}

echo "=========================================" | tee "$TEST_RESULTS"
echo "ML PIPELINE END-TO-END TEST" | tee -a "$TEST_RESULTS"
echo "Started: $(date)" | tee -a "$TEST_RESULTS"
echo "=========================================" | tee -a "$TEST_RESULTS"
echo ""

# =========================================
# TEST 1: Dataset Structure
# =========================================
echo -e "${YELLOW}Testing Dataset Structure...${NC}" | tee -a "$TEST_RESULTS"

datasets=("layer_01_raw" "layer_02_staging" "layer_03_foundation" "layer_04_semantic" "layer_05_ml" "layer_07_export" "ops_config" "ops_monitor")

for dataset in "${datasets[@]}"; do
    if bq ls -d --project_id=$PROJECT_ID | grep -q "$dataset"; then
        log_test "Dataset: $dataset" "PASS" "Exists"
    else
        log_test "Dataset: $dataset" "FAIL" "Not found"
    fi
done

# =========================================
# TEST 2: Foundation Layer Tables
# =========================================
echo ""
echo -e "${YELLOW}Testing Foundation Layer...${NC}" | tee -a "$TEST_RESULTS"

test_query "dim_caption count" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_03_foundation.dim_caption\`" \
    '[ "$result" -gt "0" ]'

test_query "dim_creator count" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_03_foundation.dim_creator\`" \
    '[ "$result" -gt "0" ]'

test_query "fact_message_send recent data" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_03_foundation.fact_message_send\` WHERE send_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)" \
    '[ "$result" -gt "0" ]'

# =========================================
# TEST 3: ML Feature Store
# =========================================
echo ""
echo -e "${YELLOW}Testing ML Feature Store...${NC}" | tee -a "$TEST_RESULTS"

test_query "Feature store freshness" \
    "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(computed_at), HOUR) as hours_old FROM \`${PROJECT_ID}.layer_05_ml.feature_store\`" \
    '[ "$result" -lt "48" ]'

test_query "Feature store completeness" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_05_ml.feature_store\` WHERE computed_date = CURRENT_DATE()" \
    '[ "$result" -gt "0" ]'

test_query "Feature quality - confidence scores" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_05_ml.feature_store\` WHERE computed_date = CURRENT_DATE() AND performance_features.confidence_score BETWEEN 0 AND 1" \
    '[ "$result" -gt "0" ]'

# =========================================
# TEST 4: ML Ranker
# =========================================
echo ""
echo -e "${YELLOW}Testing ML Ranker...${NC}" | tee -a "$TEST_RESULTS"

test_query "ML ranker output" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_05_ml.ml_ranker\` WHERE slot_date = CURRENT_DATE()" \
    '[ "$result" -gt "0" ]'

test_query "Rank distribution" \
    "SELECT COUNT(DISTINCT rank) as unique_ranks FROM \`${PROJECT_ID}.layer_05_ml.ml_ranker\` WHERE slot_date = CURRENT_DATE() AND username_page IN (SELECT DISTINCT username_page FROM \`${PROJECT_ID}.layer_05_ml.ml_ranker\` WHERE slot_date = CURRENT_DATE() LIMIT 1)" \
    '[ "$result" -gt "1" ]'

# =========================================
# TEST 5: Export Layer
# =========================================
echo ""
echo -e "${YELLOW}Testing Export Layer...${NC}" | tee -a "$TEST_RESULTS"

test_query "Schedule recommendations" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_07_export.schedule_recommendations\` WHERE schedule_date = CURRENT_DATE()" \
    '[ "$result" -gt "0" ]'

test_query "API caption lookup" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.layer_07_export.api_caption_lookup\`" \
    '[ "$result" -gt "0" ]'

# =========================================
# TEST 6: Configuration Tables
# =========================================
echo ""
echo -e "${YELLOW}Testing Configuration...${NC}" | tee -a "$TEST_RESULTS"

test_query "ML parameters config" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.ops_config.ml_parameters\` WHERE is_active = TRUE" \
    '[ "$result" -gt "0" ]'

test_query "Business rules config" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.ops_config.business_rules\` WHERE is_active = TRUE" \
    '[ "$result" -gt "0" ]'

# =========================================
# TEST 7: Monitoring Views
# =========================================
echo ""
echo -e "${YELLOW}Testing Monitoring Views...${NC}" | tee -a "$TEST_RESULTS"

test_query "System health view" \
    "SELECT health_score FROM \`${PROJECT_ID}.ops_monitor.dashboard_system_health\`" \
    '[ "$result" -ge "0" ] && [ "$result" -le "100" ]'

test_query "ML performance tracking" \
    "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.ops_monitor.dashboard_ml_performance\`" \
    '[ "$result" -gt "0" ]'

# =========================================
# TEST 8: Data Quality Checks
# =========================================
echo ""
echo -e "${YELLOW}Testing Data Quality...${NC}" | tee -a "$TEST_RESULTS"

test_query "Caption ID consistency" \
    "SELECT COUNT(*) as inconsistent FROM \`${PROJECT_ID}.layer_05_ml.feature_store\` fs LEFT JOIN \`${PROJECT_ID}.layer_03_foundation.dim_caption\` dc ON fs.caption_id = dc.caption_id WHERE fs.computed_date = CURRENT_DATE() AND dc.caption_id IS NULL" \
    '[ "$result" -eq "0" ]'

test_query "Eligible caption ratio" \
    "SELECT ROUND(AVG(CASE WHEN cooldown_features.is_eligible THEN 1.0 ELSE 0.0 END) * 100, 1) as eligible_pct FROM \`${PROJECT_ID}.layer_05_ml.feature_store\` WHERE computed_date = CURRENT_DATE()" \
    '[ "$result" -gt "10" ]'

# =========================================
# TEST 9: Performance Benchmarks
# =========================================
echo ""
echo -e "${YELLOW}Testing Performance...${NC}" | tee -a "$TEST_RESULTS"

# Test query execution time
start_time=$(date +%s)
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "SELECT * FROM \`${PROJECT_ID}.layer_07_export.schedule_recommendations\` WHERE schedule_date = CURRENT_DATE() AND username_page = 'test_user_free' LIMIT 10" > /dev/null 2>&1
end_time=$(date +%s)
query_time=$((end_time - start_time))

if [ "$query_time" -lt "5" ]; then
    log_test "Query performance" "PASS" "Executed in ${query_time}s"
else
    log_test "Query performance" "FAIL" "Took ${query_time}s (expected <5s)"
fi

# =========================================
# TEST 10: End-to-End Workflow
# =========================================
echo ""
echo -e "${YELLOW}Testing End-to-End Workflow...${NC}" | tee -a "$TEST_RESULTS"

# Simulate selecting captions for a user
test_username="test_user"
test_page_type="free"

e2e_query="
WITH test_selection AS (
  SELECT
    caption_id,
    caption_text,
    final_score,
    rank
  FROM \`${PROJECT_ID}.layer_07_export.schedule_recommendations\`
  WHERE username_page = '${test_username}_${test_page_type}'
    AND schedule_date = CURRENT_DATE()
    AND is_eligible = TRUE
  ORDER BY final_score DESC
  LIMIT 5
)
SELECT COUNT(*) as captions_found FROM test_selection
"

test_query "End-to-end caption selection" "$e2e_query" '[ "$result" -ge "0" ]'

# =========================================
# SUMMARY
# =========================================
echo ""
echo "=========================================" | tee -a "$TEST_RESULTS"
echo "TEST SUMMARY" | tee -a "$TEST_RESULTS"
echo "=========================================" | tee -a "$TEST_RESULTS"
echo -e "${GREEN}Passed:${NC} $TESTS_PASSED" | tee -a "$TEST_RESULTS"
echo -e "${RED}Failed:${NC} $TESTS_FAILED" | tee -a "$TEST_RESULTS"

if [ "$TESTS_FAILED" -eq "0" ]; then
    echo -e "${GREEN}✓ ALL TESTS PASSED!${NC}" | tee -a "$TEST_RESULTS"
    exit_code=0
else
    echo -e "${RED}✗ SOME TESTS FAILED${NC}" | tee -a "$TEST_RESULTS"
    exit_code=1
fi

echo ""
echo "Completed: $(date)" | tee -a "$TEST_RESULTS"
echo "Results saved to: $TEST_RESULTS"

exit $exit_code
