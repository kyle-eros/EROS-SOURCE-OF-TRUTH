# src/main.py
"""
Main ETL Pipeline for Gmail to BigQuery.
Fetches infloww.com reports from Gmail, processes Excel files, and loads to BigQuery.
"""

import os
import sys
import json
import logging
import traceback
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

# Import our modules
from config import Config
from gmail_client import GmailClient
from downloader import download_file, validate_excel_file
from processor import excel_to_dataframe, normalize_headers
from hash_generator import HashGenerator, add_hash_columns, remove_duplicates, get_hash_statistics
from username_mapper import map_username, get_raw_page_name
from gcp_clients import (
    load_dataframe_to_bigquery,
    save_state,
    load_state,
    check_for_duplicates,
    upload_to_gcs
)
from exceptions import (
    PipelineError,
    NoMessagesFoundError,
    NoDownloadUrlFound,
    InvalidFileError,
    BigQueryLoadError,
    DataValidationError
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'gmail_etl_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)


class GmailETLPipeline:
    """
    Main ETL Pipeline class.
    Handles the complete flow from Gmail to BigQuery.
    """
    
    def __init__(self, config: Config):
        """Initialize the pipeline with configuration."""
        self.cfg = config
        self.gmail_client = None
        self.state = {}
        self.processed_messages = set()
        self.failed_messages = set()
        self.stats = {
            'messages_found': 0,
            'messages_processed': 0,
            'messages_failed': 0,
            'rows_loaded': 0,
            'duplicates_skipped': 0
        }
        
    def initialize(self):
        """Initialize clients and load state."""
        logger.info("="*80)
        logger.info("GMAIL ETL PIPELINE INITIALIZATION")
        logger.info("="*80)
        logger.info(f"Project: {self.cfg.PROJECT_ID}")
        logger.info(f"Dataset: {self.cfg.BQ_DATASET}")
        logger.info(f"Table: {self.cfg.BQ_TABLE}")
        logger.info(f"Target Gmail: {self.cfg.TARGET_GMAIL_USER}")
        
        # Initialize Gmail client
        try:
            self.gmail_client = GmailClient(self.cfg)
            logger.info("✅ Gmail client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Gmail client: {e}")
            raise PipelineError(f"Gmail initialization failed: {e}")
        
        # Load state
        self._load_state()
        
    def _load_state(self):
        """Load pipeline state from GCS."""
        if self.cfg.RESET_STATE:
            logger.warning("🔄 RESET_STATE=true - Starting fresh")
            self.state = {}
            self.processed_messages = set()
            return
        
        try:
            state_uri = f"gs://{self.cfg.STATE_BUCKET}/{self.cfg.STATE_OBJECT_PATH}"
            self.state = load_state(state_uri)
            
            if self.state:
                self.processed_messages = set(self.state.get('processed_messages', []))
                logger.info(f"📋 Loaded state: {len(self.processed_messages)} processed messages")
            else:
                logger.info("📋 No existing state found - starting fresh")
        except Exception as e:
            logger.warning(f"Could not load state: {e}")
            self.state = {}
            self.processed_messages = set()
    
    def _save_state(self):
        """Save pipeline state to GCS."""
        try:
            self.state['processed_messages'] = list(self.processed_messages)
            self.state['failed_messages'] = list(self.failed_messages)
            self.state['last_run'] = datetime.utcnow().isoformat()
            self.state['stats'] = self.stats
            
            state_uri = f"gs://{self.cfg.STATE_BUCKET}/{self.cfg.STATE_OBJECT_PATH}"
            save_state(self.state, state_uri)
            logger.info("💾 State saved successfully")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def fetch_messages(self) -> List[Dict[str, Any]]:
        """Fetch unprocessed messages from Gmail."""
        logger.info("\n" + "="*60)
        logger.info("FETCHING GMAIL MESSAGES")
        logger.info("="*60)
        
        try:
            # Get all messages matching the search query
            messages = self.gmail_client.list_report_messages(
                max_results=self.cfg.MAX_MESSAGES_PER_RUN
            )
            
            self.stats['messages_found'] = len(messages)
            logger.info(f"Found {len(messages)} total messages")
            
            # Filter out already processed messages
            unprocessed = []
            for msg in messages:
                msg_id = msg.get('id')
                if msg_id not in self.processed_messages:
                    unprocessed.append(msg)
                else:
                    logger.debug(f"Skipping already processed message: {msg_id}")
            
            logger.info(f"📧 {len(unprocessed)} unprocessed messages to process")
            return unprocessed
            
        except NoMessagesFoundError:
            logger.info("No messages found matching search criteria")
            return []
        except Exception as e:
            logger.error(f"Failed to fetch messages: {e}")
            raise PipelineError(f"Gmail fetch failed: {e}")
    
    def process_message(self, message: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Process a single email message.
        
        Returns:
            DataFrame with processed data or None if failed
        """
        msg_id = message.get('id')
        logger.info(f"\n📨 Processing message {msg_id}")
        
        try:
            # Get message details
            msg_details = self.gmail_client.get_message_details(msg_id)
            logger.info(f"  Subject: {msg_details.get('subject', 'N/A')[:100]}")
            
            # Fetch message content
            internal_date, email_content = self.gmail_client.fetch_message(msg_id)
            
            # Extract download link and page name
            try:
                download_url, page_name = self.gmail_client.extract_report_link_and_page(
                    email_content, 
                    msg_id
                )
            except NoDownloadUrlFound:
                logger.warning(f"  ⚠️ No download URL found in message {msg_id}")
                self.failed_messages.add(msg_id)
                return None
            
            logger.info(f"  Page: '{page_name}'")
            logger.info(f"  URL: {download_url[:100]}...")
            
            # Download Excel file
            temp_dir = Path("/tmp/gmail_etl")
            temp_dir.mkdir(exist_ok=True)
            
            try:
                local_path = download_file(
                    download_url,
                    page_name,
                    msg_id,
                    str(temp_dir)
                )
                logger.info(f"  ✅ Downloaded: {local_path}")
            except Exception as e:
                logger.error(f"  ❌ Download failed: {e}")
                self.failed_messages.add(msg_id)
                return None
            
            # Validate Excel file
            if not validate_excel_file(local_path):
                logger.error(f"  ❌ Invalid Excel file: {local_path}")
                self.failed_messages.add(msg_id)
                return None
            
            # Upload raw file to GCS for backup
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                gcs_path = f"{self.cfg.GCS_PREFIX}/{page_name}_{timestamp}.xlsx"
                gcs_uri = f"gs://{self.cfg.GCS_RAW_BUCKET}/{gcs_path}"
                upload_to_gcs(local_path, gcs_uri, metadata={
                    'message_id': msg_id,
                    'page_name': page_name,
                    'processed_at': datetime.utcnow().isoformat()
                })
                logger.info(f"  📤 Backed up to: {gcs_uri}")
            except Exception as e:
                logger.warning(f"  Failed to backup to GCS: {e}")
                gcs_uri = ""  # Continue processing even if backup fails
            
            # Read and process Excel file
            try:
                df = excel_to_dataframe(local_path)
                logger.info(f"  📊 Read {len(df)} rows, {len(df.columns)} columns")
            except Exception as e:
                logger.error(f"  ❌ Failed to read Excel file: {e}")
                # Try to continue with empty report marker
                df = pd.DataFrame({
                    'message': ['[Failed to read Excel]'],
                    'sending_time': [datetime.now().strftime('%b %d, %Y at %I:%M %p')],
                    'sender': [''],
                    'status': 'error',
                    'price': ['0'],
                    'sent': [0],
                    'viewed': [0],
                    'purchased': [0],
                    'earnings': [0],
                    'withdrawn_by': ['']
                })
                logger.warning("  Created error placeholder for failed Excel read")
            
            # Clean up temp file
            try:
                os.remove(local_path)
            except:
                pass
            
            # Process the DataFrame
            df = self._process_dataframe(df, page_name, msg_id, gcs_uri)
            
            return df
            
        except Exception as e:
            logger.error(f"  ❌ Failed to process message {msg_id}: {e}")
            logger.error(traceback.format_exc())
            self.failed_messages.add(msg_id)
            return None
    
    def _process_dataframe(
        self, 
        df: pd.DataFrame, 
        page_name: str, 
        msg_id: str,
        source_file: str
    ) -> pd.DataFrame:
        """
        Process DataFrame to match staging table schema.
        Keep timestamps as strings for proper parsing later.
        """
        logger.info(f"  Processing DataFrame for {page_name}")
        
        # Normalize column names (lowercase, underscores) first for processing
        df = normalize_headers(df)
        
        # Log columns after normalization for debugging
        logger.debug(f"  Columns after normalization: {list(df.columns)}")
        
        # IMPORTANT: Keep sending_time as STRING - don't parse to datetime!
        # The infloww format is "Aug 25, 2025 at 11:00 AM SD Chu"
        if 'sending_time' in df.columns:
            # Clean the "SD Chu" suffix if present
            df['sending_time'] = df['sending_time'].astype(str)
            df['sending_time'] = df['sending_time'].str.replace('SD Chu', '', regex=False).str.strip()
            logger.debug(f"  Keeping sending_time as string for staging")
        
        # Clean money columns but keep Price as string with $ for staging
        if 'price' in df.columns:
            # Keep price as string for staging (will be parsed in transfer query)
            df['price'] = df['price'].astype(str)
        
        if 'earnings' in df.columns:
            # Clean earnings to float
            df['earnings'] = df['earnings'].astype(str).str.replace('$', '').str.replace(',', '')
            df['earnings'] = pd.to_numeric(df['earnings'], errors='coerce').fillna(0.0)
        
        # Clean numeric columns
        numeric_columns = ['sent', 'viewed', 'purchased']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
        
        # Clean text columns
        text_columns = ['message', 'sender', 'status', 'withdrawn_by']
        for col in text_columns:
            if col in df.columns:
                # Convert to string and clean
                df[col] = df[col].astype(str).replace('nan', '').replace('None', '').str.strip()
        
        # Add metadata for tracking
        df['message_id'] = msg_id
        df['source_file'] = source_file
        
        # Rename columns to match BigQuery table schema (mixed case)
        # Only rename columns that exist in the dataframe
        column_mapping = {
            'message': 'Message',
            'sending_time': 'Sending_time',
            'sender': 'Sender',
            'status': 'Status',
            'price': 'Price',
            'sent': 'Sent',
            'viewed': 'Viewed',
            'purchased': 'Purchased',
            'earnings': 'Earnings',
            'withdrawn_by': 'Withdrawn_by',
            'message_id': 'message_id',
            'source_file': 'source_file'
        }
        
        # Apply the mapping
        existing_columns = {}
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                existing_columns[old_name] = new_name
        
        df = df.rename(columns=existing_columns)
        
        # Log final columns for debugging
        logger.debug(f"  Final columns after renaming: {list(df.columns)}")
        
        # Don't add hash columns here - they'll be generated in the transfer query
        # Don't add username mapping here - it will be extracted from Message in transfer
        
        logger.info(f"  Processed {len(df)} rows with timestamp preserved as string")
        
        return df
    
    def upload_to_bigquery(self, df: pd.DataFrame) -> bool:
        """Upload DataFrame to BigQuery staging table with NEW schema."""
        if df.empty:
            logger.warning("Empty DataFrame - nothing to upload")
            return True
        
        logger.info(f"\n📤 Uploading {len(df)} rows to BigQuery staging table")
        
        # Generate ingestion metadata
        ingestion_run_id = f"python-etl-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        ingested_at = datetime.now()
        ingestion_date = datetime.now().date()
        
        # Add ingestion metadata columns
        df['ingestion_run_id'] = ingestion_run_id
        df['ingested_at'] = ingested_at
        df['ingestion_date'] = ingestion_date
        
        # Validate required fields are NOT NULL
        if 'message_id' not in df.columns or df['message_id'].isnull().any():
            logger.error("message_id contains NULL values - this is REQUIRED")
            return False
        if 'source_file' not in df.columns or df['source_file'].isnull().any():
            logger.error("source_file contains NULL values - this is REQUIRED")
            return False
        
        # Parse timestamps from Sending_time
        df['message_sent_ts'] = pd.to_datetime(df['Sending_time'], 
                                               format='%b %d, %Y at %I:%M %p', 
                                               errors='coerce')
        df['message_sent_date'] = df['message_sent_ts'].dt.date
        
        # Ensure required columns exist (matching NEW BigQuery schema)
        required_columns = [
            'ingestion_run_id', 'ingested_at', 'ingestion_date',
            'message_id', 'source_file',
            'Message', 'Sending_time', 'Sender', 'Price',
            'Sent', 'Viewed', 'Purchased', 'Earnings',
            'message_sent_ts', 'message_sent_date'
        ]
        
        # Add any missing columns with default values
        for col in required_columns:
            if col not in df.columns:
                if col in ['Sent', 'Viewed', 'Purchased']:
                    df[col] = 0
                elif col == 'Earnings':
                    df[col] = ''  # STRING in new schema
                else:
                    df[col] = ''
        
        # Select only required columns in correct order
        df = df[required_columns]
        
        # Ensure data types match NEW staging schema
        # Keep Sending_time, Price, and Earnings as STRING
        string_columns = ['ingestion_run_id', 'message_id', 'source_file',
                         'Message', 'Sending_time', 'Sender', 'Price', 'Earnings']
        for col in string_columns:
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
        
        # Ensure numeric columns are correct type
        for col in ['Sent', 'Viewed', 'Purchased']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
        
        # Replace NaN with None for BigQuery
        df = df.where(pd.notnull(df), None)
        
        # Upload to BigQuery staging table
        try:
            table_id = f"{self.cfg.PROJECT_ID}.{self.cfg.BQ_DATASET}.{self.cfg.BQ_TABLE}"
            logger.info(f"📤 Uploading to BigQuery table: {table_id}")
            
            rows_loaded = load_dataframe_to_bigquery(
                df,
                table_id,
                location=self.cfg.BIGQUERY_LOCATION,
                write_disposition="WRITE_APPEND"
            )
            
            self.stats['rows_loaded'] += rows_loaded
            logger.info(f"✅ Successfully loaded {rows_loaded} rows to staging table")
            return True
            
        except BigQueryLoadError as e:
            logger.error(f"❌ BigQuery load failed: {e}")
            self._save_debug_data(df, "bigquery_failed")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error during BigQuery upload: {e}")
            self._save_debug_data(df, "upload_error")
            return False
    
    def _save_debug_data(self, df: pd.DataFrame, prefix: str):
        """Save DataFrame to CSV for debugging."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"/tmp/{prefix}_{timestamp}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved debug data to {filename}")
        except Exception as e:
            logger.error(f"Failed to save debug data: {e}")
    
    def run(self):
        """Main pipeline execution."""
        logger.info("="*80)
        logger.info("STARTING GMAIL ETL PIPELINE")
        logger.info("="*80)
        
        try:
            # Fetch unprocessed messages
            messages = self.fetch_messages()
            
            if not messages:
                logger.info("No new messages to process")
                return
            
            # Process each message
            for message in messages:
                msg_id = message.get('id')
                
                try:
                    # Process the message
                    df = self.process_message(message)
                    
                    if df is not None and not df.empty:
                        # Upload to BigQuery
                        success = self.upload_to_bigquery(df)
                        
                        if success:
                            self.processed_messages.add(msg_id)
                            self.stats['messages_processed'] += 1
                            logger.info(f"✅ Successfully processed message {msg_id}")
                        else:
                            self.failed_messages.add(msg_id)
                            self.stats['messages_failed'] += 1
                    else:
                        logger.warning(f"No data extracted from message {msg_id}")
                        self.failed_messages.add(msg_id)
                        self.stats['messages_failed'] += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process message {msg_id}: {e}")
                    self.failed_messages.add(msg_id)
                    self.stats['messages_failed'] += 1
            
            # Save state after processing all messages
            self._save_state()
            
            # Print summary
            logger.info("\n" + "="*60)
            logger.info("PIPELINE SUMMARY")
            logger.info("="*60)
            logger.info(f"Messages found:     {self.stats['messages_found']}")
            logger.info(f"Messages processed: {self.stats['messages_processed']}")
            logger.info(f"Messages failed:    {self.stats['messages_failed']}")
            logger.info(f"Rows loaded:        {self.stats['rows_loaded']}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            logger.error(traceback.format_exc())
            raise PipelineError(f"Pipeline execution failed: {e}")
        finally:
            # Always try to save state
            self._save_state()


# Entry point
if __name__ == "__main__":
    try:
        # Load configuration
        config = Config()
        
        # Create and run pipeline
        pipeline = GmailETLPipeline(config)
        pipeline.initialize()
        pipeline.run()
        
        logger.info("✨ Pipeline completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)