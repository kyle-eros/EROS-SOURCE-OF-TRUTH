# src/gcp_clients.py
"""
Google Cloud Platform client wrappers.
Handles BigQuery, GCS, and state management.
"""

import json
import time
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound, Conflict
from google.api_core.exceptions import RetryError, DeadlineExceeded

from exceptions import BigQueryLoadError

logger = logging.getLogger(__name__)

# Initialize clients (singleton pattern)
_bigquery_client = None
_storage_client = None


def get_bigquery_client():
    """Get or create BigQuery client"""
    global _bigquery_client
    if _bigquery_client is None:
        _bigquery_client = bigquery.Client()
    return _bigquery_client


def get_storage_client():
    """Get or create Storage client"""
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


# -----------------------------------------------------------------------------
# GCS Functions
# -----------------------------------------------------------------------------

def parse_gcs_uri(gcs_uri: str) -> tuple:
    """Parse a GCS URI into bucket and object path"""
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")
    
    parts = gcs_uri[5:].split("/", 1)
    bucket = parts[0]
    object_path = parts[1] if len(parts) > 1 else ""
    
    return bucket, object_path


def upload_to_gcs(
    local_path: str,
    gcs_uri: str,
    metadata: Optional[Dict[str, str]] = None,
    content_type: Optional[str] = None
) -> str:
    """
    Upload a local file to GCS.
    
    Args:
        local_path: Path to local file
        gcs_uri: Destination GCS URI (gs://bucket/path/to/file)
        metadata: Optional metadata dict
        content_type: Optional MIME type
    
    Returns:
        The GCS URI of the uploaded object
    """
    bucket_name, object_path = parse_gcs_uri(gcs_uri)
    
    logger.debug(f"Uploading {local_path} to gs://{bucket_name}/{object_path}")
    
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    
    if metadata:
        blob.metadata = metadata
    
    blob.upload_from_filename(local_path, content_type=content_type)
    return gcs_uri


def upload_string_to_gcs(
    text_data: str,
    gcs_uri: str,
    content_type: str = "text/plain",
    metadata: Optional[Dict[str, str]] = None
) -> str:
    """
    Upload string content to GCS.
    
    Args:
        text_data: String content to upload
        gcs_uri: Destination GCS URI
        content_type: MIME type of the content
        metadata: Optional metadata
    
    Returns:
        The GCS URI of the uploaded object
    """
    bucket_name, object_path = parse_gcs_uri(gcs_uri)
    
    logger.debug(f"Uploading string data to gs://{bucket_name}/{object_path}")
    
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    
    if metadata:
        blob.metadata = metadata
    
    blob.upload_from_string(text_data, content_type=content_type)
    return gcs_uri


def download_from_gcs(gcs_uri: str) -> str:
    """Download content from GCS as string"""
    bucket_name, object_path = parse_gcs_uri(gcs_uri)
    
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    
    if not blob.exists():
        return None
    
    return blob.download_as_text()


def gcs_file_exists(gcs_uri: str) -> bool:
    """Check if a file exists in GCS"""
    bucket_name, object_path = parse_gcs_uri(gcs_uri)
    
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    
    return blob.exists()


# -----------------------------------------------------------------------------
# State Management Functions
# -----------------------------------------------------------------------------

def save_state(state_data: Dict[str, Any], state_uri: str) -> None:
    """Save pipeline state to GCS"""
    state_json = json.dumps(state_data, indent=2, default=str)
    upload_string_to_gcs(
        state_json, 
        state_uri, 
        content_type="application/json",
        metadata={"updated_at": datetime.utcnow().isoformat()}
    )
    logger.info(f"Saved state to {state_uri}")


def load_state(state_uri: str) -> Dict[str, Any]:
    """Load pipeline state from GCS"""
    try:
        state_json = download_from_gcs(state_uri)
        if state_json:
            return json.loads(state_json)
    except Exception as e:
        logger.warning(f"Could not load state: {e}")
    
    return {}


# -----------------------------------------------------------------------------
# BigQuery Functions
# -----------------------------------------------------------------------------

def ensure_bigquery_table_exists(
    table_id: str,
    schema: List[bigquery.SchemaField] = None,
    location: str = "US"
) -> bigquery.Table:
    """
    Ensure a BigQuery table exists, create if it doesn't.
    
    Args:
        table_id: Full table ID (project.dataset.table)
        schema: Table schema (if creating new)
        location: BigQuery location
    
    Returns:
        The BigQuery Table object
    """
    client = get_bigquery_client()
    
    try:
        table = client.get_table(table_id)
        logger.info(f"Table {table_id} exists with {table.num_rows} rows")
        return table
    except NotFound:
        logger.info(f"Table {table_id} not found, creating...")
        
        if not schema:
            # Check if this is the staging table
            if "staging.gmail_etl_daily" in table_id:
                schema = get_staging_bigquery_schema()
            else:
                # Default schema for other tables
                schema = get_default_bigquery_schema()
        
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, location=location)
        logger.info(f"✅ Created table {table_id}")
        return table


def get_staging_bigquery_schema() -> List[bigquery.SchemaField]:
    """Get the schema for NEW gmail_events_staging table with ingestion partitioning"""
    return [
        # Ingestion metadata (REQUIRED)
        bigquery.SchemaField("ingestion_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ingestion_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
        
        # Raw Gmail data fields
        bigquery.SchemaField("Message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Sending_time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Sender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Price", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Sent", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("Viewed", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("Purchased", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("Earnings", "STRING", mode="NULLABLE"),
        
        # Parsed timestamps
        bigquery.SchemaField("message_sent_ts", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("message_sent_date", "DATE", mode="NULLABLE"),
    ]


def get_default_bigquery_schema() -> List[bigquery.SchemaField]:
    """Get the default schema for infloww_stats table"""
    return [
        # CHANGE COLUMN NAMES TO MATCH NEW SCHEMA:
        bigquery.SchemaField("caption_text", "STRING", mode="NULLABLE"),  # was "Message"
        bigquery.SchemaField("sending_date", "DATE", mode="NULLABLE"),  # was "sending_time" 
        bigquery.SchemaField("username", "STRING", mode="NULLABLE"),  # was "page_name"
        bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),  # was "Price"
        bigquery.SchemaField("sent_count", "INTEGER", mode="NULLABLE"),  # was "Sent"
        bigquery.SchemaField("viewed_count", "INTEGER", mode="NULLABLE"),  # was "Viewed"
        bigquery.SchemaField("purchased_count", "INTEGER", mode="NULLABLE"),  # was "Purchased"
        bigquery.SchemaField("revenue", "FLOAT64", mode="NULLABLE"),  # was "Earnings"
        bigquery.SchemaField("caption_hash", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("row_key_v1", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("caption_normalized", "STRING", mode="NULLABLE"),  # was "caption_norm"
        bigquery.SchemaField("view_ratio", "FLOAT64", mode="NULLABLE"),  # NEW
        bigquery.SchemaField("sent_buy_ratio", "FLOAT64", mode="NULLABLE"),  # NEW
        bigquery.SchemaField("viewed_buy_ratio", "FLOAT64", mode="NULLABLE"),  # NEW
        bigquery.SchemaField("message_type", "STRING", mode="NULLABLE"),  # NEW
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),  # was "load_ts_utc"
    ]


def prepare_dataframe_for_bigquery(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    """
    Prepare DataFrame for BigQuery upload by fixing data types.
    This is CRITICAL to avoid schema mismatch errors.
    """
    logger.info("Preparing DataFrame for BigQuery upload")
    
    # Create a copy to avoid modifying original
    df = df.copy()
    
    # Check if this is the staging table
    if "staging.gmail_etl_daily" in table_id:
        # For staging table, keep sending_time and price as strings
        string_columns = ['message', 'sending_time', 'sender', 'status', 
                         'price', 'withdrawn_by', 'message_id', 'source_file']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
                # Replace 'nan' string with empty string
                df[col] = df[col].replace('nan', '').replace('None', '')
        
        # Handle FLOAT columns
        if 'earnings' in df.columns:
            df['earnings'] = pd.to_numeric(df['earnings'], errors='coerce').fillna(0.0)
        
        # Handle INTEGER columns
        int_columns = ['sent', 'viewed', 'purchased']
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
        
    else:
        # Original logic for other tables
        # Handle TIMESTAMP columns
        timestamp_columns = ['sending_time', 'load_ts_utc', 'loaded_at']
        for col in timestamp_columns:
            if col in df.columns:
                logger.debug(f"Converting {col} to TIMESTAMP")
                # Convert to datetime, handling various formats
                df[col] = pd.to_datetime(df[col], errors='coerce')
                # Ensure timezone-aware (BigQuery requires UTC)
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize('UTC')
                else:
                    df[col] = df[col].dt.tz_convert('UTC')
        
        # Handle FLOAT columns
        float_columns = ['Price', 'Earnings', 'price', 'earnings', 'revenue']
        for col in float_columns:
            if col in df.columns:
                logger.debug(f"Converting {col} to FLOAT")
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0.0)
        
        # Handle INTEGER columns
        int_columns = ['Sent', 'Viewed', 'Purchased', 'sent', 'viewed', 'purchased']
        for col in int_columns:
            if col in df.columns:
                logger.debug(f"Converting {col} to INTEGER")
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)
                df[col] = df[col].astype('Int64')  # Nullable integer type
        
        # Handle STRING columns - ensure they're strings
        string_columns = [
            'Message', 'Sender', 'Status', 'Withdrawn_by', 
            'page_name', 'raw_page_name', 'caption_hash', 
            'row_key_v1', 'caption_norm', 'source_file'
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
                # Replace 'nan' string with empty string
                df[col] = df[col].replace('nan', '')
    
    # Replace NaN with None for BigQuery
    df = df.replace({np.nan: None})
    
    logger.info(f"DataFrame prepared: {len(df)} rows, {len(df.columns)} columns")
    return df


def load_dataframe_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    location: str = "US",
    write_disposition: str = "WRITE_APPEND",
    max_retries: int = 3
) -> int:
    """
    Load a pandas DataFrame into BigQuery with proper type handling.
    
    Args:
        df: DataFrame to load
        table_id: Destination table (project.dataset.table)
        location: BigQuery location
        write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY
        max_retries: Number of retries for transient failures
    
    Returns:
        Number of rows loaded
    
    Raises:
        BigQueryLoadError: If load fails after retries
    """
    if df.empty:
        logger.warning("DataFrame is empty, nothing to load")
        return 0
    
    # Prepare DataFrame for BigQuery
    df = prepare_dataframe_for_bigquery(df, table_id)
    
    client = get_bigquery_client()
    
    # Ensure table exists
    table = ensure_bigquery_table_exists(table_id, location=location)
    
    # Get appropriate schema based on table
    if "staging.gmail_etl_daily" in table_id:
        schema = get_staging_bigquery_schema()
    else:
        schema = get_default_bigquery_schema()
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        ignore_unknown_values=False,  # Be strict about schema
        max_bad_records=0,  # Don't allow bad records
    )
    
    # Attempt load with retries
    last_error = None
    for attempt in range(max_retries):
        try:
            logger.info(f"Loading {len(df)} rows to {table_id} (attempt {attempt + 1}/{max_retries})")
            
            job = client.load_table_from_dataframe(
                df,
                table_id,
                job_config=job_config,
                location=location
            )
            
            # Wait for job to complete
            result = job.result(timeout=300)
            
            rows_loaded = getattr(job, "output_rows", len(df))
            logger.info(f"✅ Successfully loaded {rows_loaded} rows to BigQuery")
            
            return rows_loaded
            
        except Exception as e:
            last_error = e
            logger.error(f"Load attempt {attempt + 1} failed: {e}")
            
            # Check if it's a schema mismatch error
            if "schema does not match" in str(e).lower():
                raise BigQueryLoadError(
                    table_id=table_id,
                    reason=f"Schema mismatch: {e}",
                    row_count=len(df)
                )
            
            # Wait before retry
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
    
    # All retries failed
    raise BigQueryLoadError(
        table_id=table_id,
        reason=f"Failed after {max_retries} attempts: {last_error}",
        row_count=len(df)
    )


def query_bigquery(query: str, location: str = "US") -> pd.DataFrame:
    """Execute a BigQuery query and return results as DataFrame"""
    client = get_bigquery_client()
    
    logger.debug(f"Executing query: {query[:200]}...")
    query_job = client.query(query, location=location)
    
    return query_job.to_dataframe()


def check_for_duplicates(
    table_id: str,
    row_keys: List[str],
    key_column: str = "row_key_v1"
) -> List[str]:
    """
    Check if row keys already exist in BigQuery table.
    
    Returns:
        List of row keys that already exist (duplicates)
    """
    if not row_keys:
        return []
    
    client = get_bigquery_client()
    
    # Build query
    keys_str = "', '".join(row_keys[:1000])  # Limit to 1000 for query size
    query = f"""
    SELECT DISTINCT {key_column}
    FROM `{table_id}`
    WHERE {key_column} IN ('{keys_str}')
    """
    
    try:
        result_df = query_bigquery(query)
        existing_keys = result_df[key_column].tolist() if not result_df.empty else []
        
        if existing_keys:
            logger.info(f"Found {len(existing_keys)} duplicate row keys")
        
        return existing_keys
    except Exception as e:
        logger.warning(f"Could not check for duplicates: {e}")
        return []