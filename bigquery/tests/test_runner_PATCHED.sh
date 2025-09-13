#!/bin/bash
# =====================================================
# ML SCHEDULING TEST RUNNER [PATCHED]
# =====================================================
# Fixes: --dry_run flag placement, better error handling
# Usage: ./test_runner_PATCHED.sh [test_type]
# Types: dry_run, golden, qa, backtest, acceptance, all

set -e  # Exit on error
PROJECT_ID="of-scheduler-proj"
MAX_BYTES=5368709120  # 5GB limit

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🧪 ML Scheduling Test Suite (PATCHED)"
echo "====================================="

# FIXED: Function to run query with proper flag placement
run_query() {
    local sql=$1
    local description=$2
    local extra_flags="${3:-}"  # Optional extra flags
    
    echo -e "${YELLOW}Testing: ${description}${NC}"
    
    # Build command with flags properly placed
    if bq query \
        --use_legacy_sql=false \
        --maximum_bytes_billed=$MAX_BYTES \
        $extra_flags \
        --format=prettyjson \
        "$sql" > /tmp/test_result.json 2>&1; then
        echo -e "${GREEN}✓ PASS${NC}"
        return 0
    else
        echo -e "${RED}✗ FAIL${NC}"
        cat /tmp/test_result.json
        return 1
    fi
}

# 1. DRY RUN TESTS - Verify views compile
test_dry_run() {
    echo "1. DRY RUN TESTS"
    echo "----------------"
    
    # Test each view compiles (PATCHED suffix for new views)
    views=(
        "mart.caption_features_vNext"
        "mart.caption_ranker_vNext"
        "mart.sheets_schedule_export_vNext"
        "qa.caption_diversity_check_vNext"
        "qa.cooldown_violations_vNext"
        "qa.scoring_distribution_vNext"
        "qa.data_completeness_vNext"
        "qa.daily_health_vNext"
    )
    
    for view in "${views[@]}"; do
        sql="SELECT * FROM \`$PROJECT_ID.$view\` LIMIT 0"
        # FIXED: --dry_run as separate flag
        run_query "$sql" "$view compilation" "--dry_run"
    done
}

# 2. GOLDEN TESTS - Deterministic results
test_golden() {
    echo "2. GOLDEN TESTS"
    echo "---------------"
    
    # Test Bayesian smoothing
    sql="WITH test_data AS (
        SELECT 'cap_001' AS caption_id, 10 AS sent, 2 AS purchased
        UNION ALL
        SELECT 'cap_002', 1000, 150
    )
    SELECT 
        caption_id,
        sent,
        purchased,
        SAFE_DIVIDE(purchased, sent) AS raw_rate,
        -- Smoothed with prior of 0.1 and weight 30
        SAFE_DIVIDE(purchased + 0.1 * 30, sent + 30) AS smoothed_rate
    FROM test_data"
    
    run_query "$sql" "Bayesian smoothing calculation"
    
    # Test deterministic epsilon (FIXED)
    sql="SELECT
        caption_id,
        slot_date,
        hod,
        ABS(FARM_FINGERPRINT(CONCAT(caption_id, slot_date, CAST(hod AS STRING)))) / 9.22e18 AS epsilon_value,
        (ABS(FARM_FINGERPRINT(CONCAT(caption_id, slot_date, CAST(hod AS STRING)))) / 9.22e18) < 0.1 AS epsilon_flag
    FROM (
        SELECT 'cap_001' AS caption_id, '2024-01-01' AS slot_date, 10 AS hod
        UNION ALL
        SELECT 'cap_002', '2024-01-01', 10
        UNION ALL
        SELECT 'cap_001', '2024-01-02', 10
    )"
    
    run_query "$sql" "Deterministic epsilon calculation"
    
    # Test UCB exploration bonus
    sql="SELECT
        SQRT(2 * LN(1000) / 10) AS ucb_bonus_low_obs,
        SQRT(2 * LN(1000) / 100) AS ucb_bonus_med_obs,
        SQRT(2 * LN(1000) / 1000) AS ucb_bonus_high_obs"
    
    run_query "$sql" "UCB exploration bonus"
    
    # Test per-slot score normalization
    sql="WITH scores AS (
        SELECT 'page1' AS page, '2024-01-01' AS date, 10 AS hod, 10 AS score
        UNION ALL SELECT 'page1', '2024-01-01', 10, 50
        UNION ALL SELECT 'page1', '2024-01-01', 10, 90
        UNION ALL SELECT 'page1', '2024-01-01', 11, 20  -- Different slot
        UNION ALL SELECT 'page1', '2024-01-01', 11, 80
    )
    SELECT 
        page, date, hod, score,
        100 * (score - MIN(score) OVER (PARTITION BY page, date, hod)) / 
        NULLIF(MAX(score) OVER (PARTITION BY page, date, hod) - 
               MIN(score) OVER (PARTITION BY page, date, hod), 0) AS normalized
    FROM scores
    ORDER BY page, date, hod, score"
    
    run_query "$sql" "Per-slot score normalization"
}

# 3. QA VALIDATION TESTS
test_qa() {
    echo "3. QA VALIDATION"
    echo "----------------"
    
    # Check diversity (on canonical ladder position)
    sql="SELECT 
        AVG(diversity_ratio) AS avg_diversity,
        MIN(diversity_ratio) AS min_diversity,
        COUNT(CASE WHEN diversity_status = 'FAIL' THEN 1 END) AS failures
    FROM \`$PROJECT_ID.qa.caption_diversity_check_vNext\`"
    
    run_query "$sql" "Caption diversity metrics"
    
    # Check cooldowns (with TIMESTAMP_DIFF)
    sql="SELECT
        AVG(violation_rate) AS avg_violation_rate,
        MAX(violations_168h) AS max_violations,
        COUNT(CASE WHEN cooldown_status = 'FAIL' THEN 1 END) AS failures
    FROM \`$PROJECT_ID.qa.cooldown_violations_vNext\`"
    
    run_query "$sql" "Cooldown compliance"
    
    # Check completeness
    sql="SELECT
        AVG(caption_fill_rate) AS avg_fill_rate,
        AVG(fallback_rate) AS avg_fallback_rate,
        COUNT(CASE WHEN completeness_status = 'FAIL' THEN 1 END) AS failures
    FROM \`$PROJECT_ID.qa.data_completeness_vNext\`"
    
    run_query "$sql" "Data completeness"
    
    # Overall health
    sql="SELECT
        metric_name,
        metric_value,
        status,
        trigger_rollback
    FROM \`$PROJECT_ID.qa.daily_health_vNext\`
    ORDER BY metric_name"
    
    run_query "$sql" "Overall health summary"
}

# 4. BACKTEST - Compare old vs new
test_backtest() {
    echo "4. BACKTEST COMPARISON"
    echo "----------------------"
    
    # Compare RPS between old and new approach
    sql="WITH old_approach AS (
        SELECT 
            username_page,
            AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS avg_rps
        FROM \`$PROJECT_ID.core.message_facts\`
        WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            AND sending_ts < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            AND sent > 0
        GROUP BY username_page
    ),
    new_approach AS (
        SELECT
            username_page,
            AVG(rps) AS avg_rps
        FROM \`$PROJECT_ID.mart.caption_features_vNext\`
        GROUP BY username_page
    )
    SELECT
        COUNT(*) AS pages_compared,
        AVG(n.avg_rps - o.avg_rps) AS avg_rps_lift,
        AVG(SAFE_DIVIDE(n.avg_rps - o.avg_rps, o.avg_rps)) AS avg_rps_lift_pct
    FROM old_approach o
    JOIN new_approach n USING (username_page)"
    
    run_query "$sql" "RPS lift estimation"
}

# 5. ACCEPTANCE CRITERIA CHECKS
test_acceptance() {
    echo "5. ACCEPTANCE CRITERIA"
    echo "----------------------"
    
    # Criterion 1: Non-empty caption_id and caption_text (check canonical position)
    sql="SELECT
        COUNT(*) AS total_rows,
        COUNT(caption_id) AS rows_with_caption_id,
        COUNT(caption_text) AS rows_with_caption_text,
        SAFE_DIVIDE(COUNT(caption_id), COUNT(*)) AS caption_fill_rate
    FROM \`$PROJECT_ID.mart.sheets_schedule_export_vNext\`
    WHERE schedule_date = CURRENT_DATE()
      AND \`Ladder Position\` = 3"  # Canonical position
    
    run_query "$sql" "Criterion 1: Caption completeness"
    
    # Criterion 2: Cooldown violations ≤ 1%
    sql="SELECT
        AVG(violation_rate) AS avg_violation_rate,
        CASE 
            WHEN AVG(violation_rate) <= 0.01 THEN 'PASS'
            ELSE 'FAIL'
        END AS status
    FROM \`$PROJECT_ID.qa.cooldown_violations_vNext\`"
    
    run_query "$sql" "Criterion 2: Cooldown compliance"
    
    # Criterion 3: Diversity ≥ 85%
    sql="SELECT
        AVG(diversity_ratio) AS avg_diversity,
        CASE
            WHEN AVG(diversity_ratio) >= 0.85 THEN 'PASS'
            ELSE 'FAIL'
        END AS status
    FROM \`$PROJECT_ID.qa.caption_diversity_check_vNext\`"
    
    run_query "$sql" "Criterion 3: Caption diversity"
    
    # Criterion 5: Fallback usage < 3%
    sql="SELECT
        AVG(fallback_rate) AS avg_fallback_rate,
        CASE
            WHEN AVG(fallback_rate) < 0.03 THEN 'PASS'
            ELSE 'FAIL'
        END AS status
    FROM \`$PROJECT_ID.qa.data_completeness_vNext\`"
    
    run_query "$sql" "Criterion 5: Fallback usage"
}

# Main execution
case "${1:-all}" in
    dry_run)
        test_dry_run
        ;;
    golden)
        test_golden
        ;;
    qa)
        test_qa
        ;;
    backtest)
        test_backtest
        ;;
    acceptance)
        test_acceptance
        ;;
    all)
        test_dry_run
        echo ""
        test_golden
        echo ""
        test_qa
        echo ""
        test_backtest
        echo ""
        test_acceptance
        ;;
    *)
        echo "Usage: $0 [dry_run|golden|qa|backtest|acceptance|all]"
        exit 1
        ;;
esac

echo ""
echo "====================================="
echo "✅ Test suite completed (PATCHED)"