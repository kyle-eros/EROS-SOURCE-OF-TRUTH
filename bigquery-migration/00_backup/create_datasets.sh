#!/bin/bash

# Create new BigQuery datasets with proper naming conventions
# This script creates the clean architecture for the ML scheduling system

PROJECT_ID="of-scheduler-proj"
LOCATION="US"

echo "========================================="
echo "Creating New BigQuery Dataset Architecture"
echo "========================================="

# Data layers
echo "Creating data layer datasets..."

bq mk --dataset \
  --location=$LOCATION \
  --description="Layer 1: Raw data ingestion - exact copies of source data" \
  ${PROJECT_ID}:layer_01_raw

bq mk --dataset \
  --location=$LOCATION \
  --description="Layer 2: Staging - cleaned, typed, deduplicated data" \
  ${PROJECT_ID}:layer_02_staging

bq mk --dataset \
  --location=$LOCATION \
  --description="Layer 3: Foundation - core business entities and facts" \
  ${PROJECT_ID}:layer_03_foundation

bq mk --dataset \
  --location=$LOCATION \
  --description="Layer 4: Semantic - business logic and calculated metrics" \
  ${PROJECT_ID}:layer_04_semantic

bq mk --dataset \
  --location=$LOCATION \
  --description="Layer 5: ML - feature engineering and model predictions" \
  ${PROJECT_ID}:layer_05_ml

bq mk --dataset \
  --location=$LOCATION \
  --description="Layer 6: Analytics - ready-to-use dashboards and reports" \
  ${PROJECT_ID}:layer_06_analytics

bq mk --dataset \
  --location=$LOCATION \
  --description="Layer 7: Export - application interfaces and APIs" \
  ${PROJECT_ID}:layer_07_export

# Operational datasets
echo "Creating operational datasets..."

bq mk --dataset \
  --location=$LOCATION \
  --description="Configuration management - ML parameters and feature flags" \
  ${PROJECT_ID}:ops_config

bq mk --dataset \
  --location=$LOCATION \
  --description="System monitoring - data quality and performance tracking" \
  ${PROJECT_ID}:ops_monitor

bq mk --dataset \
  --location=$LOCATION \
  --description="Audit logging - data lineage and access tracking" \
  ${PROJECT_ID}:ops_audit

echo "========================================="
echo "✓ Dataset creation complete!"
echo "========================================="