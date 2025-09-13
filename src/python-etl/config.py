import os
from typing import Optional

class Config:
    """Configuration for Gmail ETL pipeline - centralized settings"""
    
    def __init__(self):
        # GCP Project Settings
        self.PROJECT_ID = os.environ.get('PROJECT_ID', 'of-scheduler-proj')
        
        # Gmail API Settings
        self.TARGET_GMAIL_USER = os.environ.get('TARGET_GMAIL_USER', 'kyle@erosops.com')
        self.GMAIL_SEARCH_QUERY = os.environ.get('GMAIL_SEARCH_QUERY', 
            'from:no-reply@infloww.com subject:"OF mass message history report is ready for download" newer_than:4d')
        
        # Service Account Settings (for Gmail domain-wide delegation)
        self.SERVICE_ACCOUNT_EMAIL = os.environ.get('SERVICE_ACCOUNT_EMAIL', 
            'gmail-elt-pipeline-sa@of-scheduler-proj.iam.gserviceaccount.com')
        self.GMAIL_SERVICE_ACCOUNT_FILE = os.environ.get('GMAIL_SERVICE_ACCOUNT_FILE', '')
        
        # ============================================
        # 🔥 UPDATED BIGQUERY SETTINGS 🔥
        # ============================================
        # BigQuery Settings - UPDATED FOR NEW INGESTION-PARTITIONED TABLE
        self.BQ_DATASET = os.environ.get('BQ_DATASET', 'layer_02_staging')  # New layer-based dataset
        self.BQ_TABLE = os.environ.get('BQ_TABLE', 'gmail_events_staging')  # New partitioned table
        self.BIGQUERY_LOCATION = os.environ.get('BIGQUERY_LOCATION', 'US')
        
        # GCS Settings (for storing raw files and state)
        self.GCS_RAW_BUCKET = os.environ.get('GCS_RAW_BUCKET', 'eros-report-files-raw-2025')
        self.GCS_PREFIX = os.environ.get('GCS_PREFIX', 'gmail_etl/reports')
        
        # ============================================
        # STATE MANAGEMENT (NO CHANGES NEEDED)
        # ============================================
        # State Management (to track processed emails) - STAYS THE SAME
        self.STATE_BUCKET = os.environ.get('STATE_BUCKET', self.GCS_RAW_BUCKET)
        self.STATE_OBJECT_PATH = os.environ.get('STATE_OBJECT_PATH', 'state/gmail_last_processed.json')
        
        # Processing Settings
        self.LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
        self.RESET_STATE = os.environ.get('RESET_STATE', 'false').lower() == 'true'
        self.MAX_MESSAGES_PER_RUN = int(os.environ.get('MAX_MESSAGES_PER_RUN', '500'))
    
    def __repr__(self):
        return f"Config(project={self.PROJECT_ID}, dataset={self.BQ_DATASET}, table={self.BQ_TABLE})"