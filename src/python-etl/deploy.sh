#!/usr/bin/env bash
# Gmail ETL Pipeline - Deployment Script
# Deploy to Cloud Run and optionally configure scheduler

set -euo pipefail

# Configuration
ENV_FILE="${ENV_FILE:-env.yaml}"
FULL_RUN=0
DRY_RUN=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --full)
            FULL_RUN=1
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --env)
            ENV_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--full] [--dry-run] [--env env.yaml]"
            exit 1
            ;;
    esac
done

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

# Load environment variables from YAML
load_yaml() {
    python3 - "$ENV_FILE" <<'PYTHON'
import sys
import yaml

env_file = sys.argv[1]
try:
    with open(env_file, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}
    
    for key, value in data.items():
        if value is not None:
            # Escape quotes in values
            if isinstance(value, str):
                value = value.replace('"', '\\"')
            print(f'export {key}="{value}"')
except Exception as e:
    print(f"echo 'Error loading YAML: {e}'", file=sys.stderr)
    sys.exit(1)
PYTHON
}

# Load environment
if [[ -f "$ENV_FILE" ]]; then
    log_info "Loading configuration from $ENV_FILE"
    eval "$(load_yaml)"
else
    log_error "$ENV_FILE not found"
    exit 1
fi

# Validate required configuration
require() {
    local var="$1"
    if [[ -z "${!var:-}" ]]; then
        log_error "Missing required configuration: $var"
        exit 1
    fi
}

log_info "Validating configuration..."
require PROJECT_ID
require CLOUD_RUN_REGION
require JOB_NAME
require CLOUD_RUN_SERVICE_ACCOUNT
require TARGET_GMAIL_USER
require BQ_DATASET
require BQ_TABLE

# Build deployment flags
DEPLOY_FLAGS=(
    "--source" "."
    "--project" "${PROJECT_ID}"
    "--region" "${CLOUD_RUN_REGION}"
    "--service-account" "${CLOUD_RUN_SERVICE_ACCOUNT}"
    "--env-vars-file" "${ENV_FILE}"
    "--memory" "2Gi"  # Increase memory for processing large Excel files
    "--cpu" "2"       # Use 2 CPUs for better performance
    "--task-timeout" "3600"  # 1 hour timeout
    "--parallelism" "1"  # Single instance
    "--max-retries" "2"  # Retry failed executions
)

# Add secret if configured
if [[ -n "${GMAIL_SA_SECRET_NAME:-}" ]]; then
    DEPLOY_FLAGS+=("--set-secrets=GMAIL_SA_JSON=${GMAIL_SA_SECRET_NAME}:latest")
    log_info "Using Secret Manager for Gmail authentication"
fi

# Dry run check
if [[ $DRY_RUN -eq 1 ]]; then
    log_warn "DRY RUN MODE - Commands will be shown but not executed"
    echo "gcloud run jobs deploy ${JOB_NAME} ${DEPLOY_FLAGS[@]}"
    exit 0
fi

# Deploy the Cloud Run Job
log_info "Deploying Cloud Run Job '${JOB_NAME}'..."
if gcloud run jobs deploy "${JOB_NAME}" "${DEPLOY_FLAGS[@]}" --quiet; then
    log_info "Cloud Run Job deployed successfully"
else
    log_error "Deployment failed"
    exit 1
fi

# Handle full run (reset state)
if [[ $FULL_RUN -eq 1 ]]; then
    log_warn "Full run requested - will reset state and reprocess all messages"
    
    # Clear state file
    if [[ -n "${STATE_BUCKET:-}" && -n "${STATE_OBJECT_PATH:-}" ]]; then
        log_info "Clearing state file..."
        gsutil rm -f "gs://${STATE_BUCKET}/${STATE_OBJECT_PATH}" 2>/dev/null || true
    fi
    
    # Execute with RESET_STATE=true
    log_info "Executing job with RESET_STATE=true..."
    gcloud run jobs execute "${JOB_NAME}" \
        --region "${CLOUD_RUN_REGION}" \
        --project "${PROJECT_ID}" \
        --update-env-vars "RESET_STATE=true" \
        --wait
    
    # Reset the env var back
    gcloud run jobs update "${JOB_NAME}" \
        --region "${CLOUD_RUN_REGION}" \
        --project "${PROJECT_ID}" \
        --update-env-vars "RESET_STATE=false" \
        --quiet
fi

# Configure Cloud Scheduler (if enabled)
if [[ -n "${SCHEDULER_SA:-}" && -n "${CRON_SCHEDULE:-}" ]]; then
    log_info "Configuring Cloud Scheduler..."
    
    SCHEDULER_JOB_NAME="${JOB_NAME}-scheduler"
    JOB_URI="https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/${CLOUD_RUN_REGION}/jobs/${JOB_NAME}:run"
    
    log_info "Cloud Run Job URI: ${JOB_URI}"
    
    # Check if scheduler job exists
    if gcloud scheduler jobs describe "${SCHEDULER_JOB_NAME}" \
        --location "${SCHEDULER_REGION:-us-central1}" \
        --project "${PROJECT_ID}" &>/dev/null; then
        
        log_info "Updating existing scheduler job..."
        gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
            --location "${SCHEDULER_REGION:-us-central1}" \
            --schedule "${CRON_SCHEDULE}" \
            --time-zone "${SCHEDULER_TIME_ZONE:-America/Denver}" \
            --uri "${JOB_URI}" \
            --http-method POST \
            --oidc-service-account-email "${SCHEDULER_SA}" \
            --oidc-token-audience "${JOB_URI}" \
            --project "${PROJECT_ID}" \
            --quiet
    else
        log_info "Creating new scheduler job..."
        gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
            --location "${SCHEDULER_REGION:-us-central1}" \
            --schedule "${CRON_SCHEDULE}" \
            --time-zone "${SCHEDULER_TIME_ZONE:-America/Denver}" \
            --uri "${JOB_URI}" \
            --http-method POST \
            --oidc-service-account-email "${SCHEDULER_SA}" \
            --oidc-token-audience "${JOB_URI}" \
            --project "${PROJECT_ID}" \
            --quiet
    fi
    
    log_info "Scheduler configured for: ${CRON_SCHEDULE}"
fi

# Print helpful commands
echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo ""
echo "Useful commands:"
echo ""
echo "# Execute the job manually:"
echo "gcloud run jobs execute ${JOB_NAME} \\"
echo "    --region ${CLOUD_RUN_REGION} \\"
echo "    --project ${PROJECT_ID}"
echo ""
echo "# View latest execution logs:"
echo "gcloud run jobs executions list \\"
echo "    --job ${JOB_NAME} \\"
echo "    --region ${CLOUD_RUN_REGION} \\"
echo "    --project ${PROJECT_ID} \\"
echo "    --limit 1"
echo ""
echo "# Stream logs:"
echo "gcloud alpha run jobs executions logs tail \\"
echo "    \$(gcloud run jobs executions list --job ${JOB_NAME} --region ${CLOUD_RUN_REGION} --project ${PROJECT_ID} --format='value(name)' --limit 1) \\"
echo "    --region ${CLOUD_RUN_REGION} \\"
echo "    --project ${PROJECT_ID}"
echo ""
echo "# Check job status:"
echo "gcloud run jobs describe ${JOB_NAME} \\"
echo "    --region ${CLOUD_RUN_REGION} \\"
echo "    --project ${PROJECT_ID}"
echo ""