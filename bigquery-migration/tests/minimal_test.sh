#!/bin/bash
set -e

# Color codes for output
GREEN='\033[0;32m'
NC='\033[0m'

# Test counter
TESTS_PASSED=0

# Function to log test results
log_test() {
    local test_name=$1
    local status=$2
    local message=$3

    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}✓${NC} $test_name: $message"
        let TESTS_PASSED++
    fi
}

# The loop from the script
PROJECT_ID="of-scheduler-proj"
datasets=("layer_01_raw" "layer_02_staging") # Just two for speed

for dataset in "${datasets[@]}"; do
    if bq ls -d --project_id=$PROJECT_ID | grep -q "$dataset"; then
        log_test "Dataset: $dataset" "PASS" "Exists"
    else
        log_test "Dataset: $dataset" "FAIL" "Not found"
    fi
done

echo "Loop finished"