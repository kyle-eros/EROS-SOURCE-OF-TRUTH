#!/bin/bash
set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────
: "${PROJECT_ID:?Set PROJECT_ID env var (e.g., export PROJECT_ID=of-scheduler-proj)}"
LOCATION="${LOCATION:-US}"
ARCHIVE_DATE="$(date +%Y%m%d)"

# Modes: default = archive then delete; set one of these to skip a phase
ARCHIVE_ONLY="${ARCHIVE_ONLY:-false}"   # e.g., ARCHIVE_ONLY=true bash migration/run_deprecation.sh
DELETE_ONLY="${DELETE_ONLY:-false}"     # e.g., DELETE_ONLY=true  bash migration/run_deprecation.sh

# ── Prereqs ─────────────────────────────────────────────────────────
command -v jq >/dev/null 2>&1 || { echo >&2 "jq is required but not installed. Aborting."; exit 1; }

echo "Starting deprecation process for project: $PROJECT_ID (location: $LOCATION)"
echo "Archive date tag: ${ARCHIVE_DATE}"
echo

dataset_exists() {
  bq --location="$LOCATION" --project_id="$PROJECT_ID" show --dataset "${PROJECT_ID}:$1" >/dev/null 2>&1
}

archive_one() {
  local DS="$1"
  local ARCHIVE_DS="archive_${ARCHIVE_DATE}_${DS}"

  if ! dataset_exists "$DS"; then
    echo "⚠️  Dataset '${DS}' not found; skipping archive."
    return 0
  fi

  echo "• Creating archive dataset: ${ARCHIVE_DS}"
  bq --location="$LOCATION" --project_id="$PROJECT_ID" mk --dataset "${PROJECT_ID}:${ARCHIVE_DS}" >/dev/null 2>&1 || true

  echo "• Listing tables in '${DS}'..."
  # Collect table names; if none, skip copy step
  TABLES=()
  while IFS= read -r line; do
    TABLES+=("$line")
  done < <(bq --location="$LOCATION" --project_id="$PROJECT_ID" ls --format=json "${PROJECT_ID}:${DS}" \
    | jq -r '.[] | select(.type=="TABLE") | .tableId')

  local COUNT="${#TABLES[@]}"
  echo "  Found ${COUNT} table(s)."
  if (( COUNT == 0 )); then
    echo "  No tables to archive in '${DS}'."
  else
    for T in "${TABLES[@]}"; do
      echo "  ${DS}.${T}  ->  ${ARCHIVE_DS}.${T}"
      bq --location="$LOCATION" --project_id="$PROJECT_ID" cp -f \
         "${PROJECT_ID}:${DS}.${T}" "${PROJECT_ID}:${ARCHIVE_DS}.${T}"
    done
  fi
  echo
}

delete_one() {
  local DS="$1"
  if ! dataset_exists "$DS"; then
    echo "  (skip) dataset ${DS} not found"
    return 0
  fi
  echo "Deleting dataset '${DS}' (recursive, force)..."
  bq --location="$LOCATION" --project_id="$PROJECT_ID" rm -r -f --dataset "${PROJECT_ID}:${DS}"
}

# ── Phase 1: Archive (tables only, per-dataset) ─────────────────────
if [[ "$DELETE_ONLY" != "true" ]]; then
  echo "===== ARCHIVE PHASE ====="
  for DS in core mart staging raw; do
    echo "Archiving dataset '${DS}'..."
    archive_one "$DS"
  done
  echo "✅ ARCHIVE COMPLETE."
  echo
fi

# ── Phase 2: Confirm & Delete legacy datasets ───────────────────────
if [[ "$ARCHIVE_ONLY" != "true" ]]; then
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "The next step will PERMANENTLY DELETE the original legacy datasets:"
  echo "  core, mart, staging, raw"
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

  # Support non-interactive confirm (e.g., AUTO_CONFIRM=DELETE bash migration/run_deprecation.sh)
  if [[ -n "${AUTO_CONFIRM:-}" ]]; then
    CONFIRMATION="$AUTO_CONFIRM"
    echo "AUTO_CONFIRM detected, proceeding with value: $CONFIRMATION"
  else
    read -p "Are you sure you want to proceed? (Type 'DELETE' to confirm): " CONFIRMATION
  fi

  if [[ "$CONFIRMATION" != "DELETE" ]]; then
    echo "Aborting deletion."
    exit 1
  fi

  echo
  echo "===== DELETION PHASE ====="
  for DS in core mart staging raw; do
    delete_one "$DS"
  done
  echo "✅ DEPRECATION COMPLETE."
fi
