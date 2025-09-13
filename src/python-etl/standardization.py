# src/standardization.py
"""
Column standardization module for Gmail ETL pipeline.
Maps various column name variations to standard BigQuery schema.
"""

import re
import logging
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class ColumnStandardizer:
    """
    Handles column name standardization for the ETL pipeline.
    Maps various infloww.com report formats to our BigQuery schema.
    """
    
    # Standard BigQuery column names (target schema)
    BIGQUERY_COLUMNS = [
        'Message',
        'sending_time',
        'Sender',
        'Status',
        'Price',
        'Sent',
        'Viewed',
        'Purchased',
        'Earnings',
        'Withdrawn_by',
        'page_name',
        'raw_page_name',
        'caption_hash',
        'row_key_v1',
        'caption_norm',
        'source_file',
        'load_ts_utc'
    ]
    
    # Column mapping: BigQuery name -> possible variations in Excel files
    COLUMN_MAPPINGS = {
        # Message content
        'Message': [
            'Message', 'message',
            'Caption', 'caption',
            'Text', 'text',
            'Content', 'content',
            'caption_text', 'message_text',
            'Caption Text', 'Message Text'
        ],
        
        # Timestamp when message was sent
        'sending_time': [
            'Sending_time', 'sending_time',
            'Sending time', 'SendingTime',
            'Date', 'date',
            'Timestamp', 'timestamp',
            'Send Date', 'send_date',
            'DateTime', 'datetime',
            'scheduled_datetime'
        ],
        
        # Who sent/scheduled the message
        'Sender': [
            'Sender', 'sender',
            'Scheduler', 'scheduler',
            'Sent by', 'sent_by',
            'SentBy', 'sentby',
            'Scheduled by', 'scheduled_by',
            'User', 'user',
            'scheduler_name'
        ],
        
        # Message status
        'Status': [
            'Status', 'status',
            'State', 'state',
            'Message Status', 'message_status',
            'Send Status', 'send_status'
        ],
        
        # Price of the message
        'Price': [
            'Price', 'price',
            'Amount', 'amount',
            'Cost', 'cost',
            'Message Price', 'message_price',
            'PPV Price', 'ppv_price'
        ],
        
        # Number of messages sent
        'Sent': [
            'Sent', 'sent',
            'Sent Count', 'sent_count',
            'Messages Sent', 'messages_sent',
            'Total Sent', 'total_sent',
            'Delivered', 'delivered'
        ],
        
        # Number of views
        'Viewed': [
            'Viewed', 'viewed',
            'Views', 'views',
            'View Count', 'view_count',
            'Opened', 'opened',
            'Read', 'read',
            'Total Views', 'total_views'
        ],
        
        # Number of purchases
        'Purchased': [
            'Purchased', 'purchased',
            'Purchases', 'purchases',
            'Purchase Count', 'purchase_count',
            'Bought', 'bought',
            'Sales', 'sales',
            'Conversions', 'conversions'
        ],
        
        # Total earnings from message
        'Earnings': [
            'Earnings', 'earnings',
            'Revenue', 'revenue',
            'Total', 'total',
            'Total Earnings', 'total_earnings',
            'Income', 'income',
            'Gross', 'gross'
        ],
        
        # Who withdrew the earnings
        'Withdrawn_by': [
            'Withdrawn_by', 'withdrawn_by',
            'Withdrawn by', 'WithdrawnBy',
            'Withdrawn', 'withdrawn',
            'Paid To', 'paid_to',
            'Payout', 'payout'
        ]
    }
    
    # Columns that should be cleaned/processed
    MONEY_COLUMNS = ['Price', 'Earnings']
    INTEGER_COLUMNS = ['Sent', 'Viewed', 'Purchased']
    DATETIME_COLUMNS = ['sending_time']
    TEXT_COLUMNS = ['Message', 'Sender', 'Status', 'Withdrawn_by']
    
    @staticmethod
    def find_matching_column(
        df_columns: List[str],
        variations: List[str]
    ) -> Optional[str]:
        """
        Find the first matching column from a list of variations.
        Case-insensitive matching.
        """
        # Create lowercase mapping
        column_map = {col.lower(): col for col in df_columns}
        
        # Check each variation
        for variation in variations:
            if variation.lower() in column_map:
                return column_map[variation.lower()]
        
        return None
    
    @classmethod
    def standardize_columns(
        cls,
        df: pd.DataFrame,
        strict: bool = False
    ) -> pd.DataFrame:
        """
        Standardize DataFrame columns to match BigQuery schema.
        
        Args:
            df: DataFrame to standardize
            strict: If True, only keep columns that map to BigQuery schema
            
        Returns:
            DataFrame with standardized column names
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to standardize_columns")
            return df
        
        logger.info(f"Standardizing {len(df.columns)} columns")
        original_columns = list(df.columns)
        
        # Build mapping from existing columns to BigQuery names
        column_mapping = {}
        unmapped_columns = []
        
        for bq_name, variations in cls.COLUMN_MAPPINGS.items():
            matched_col = cls.find_matching_column(df.columns, variations)
            if matched_col:
                column_mapping[matched_col] = bq_name
                logger.debug(f"  Mapped '{matched_col}' -> '{bq_name}'")
        
        # Handle unmapped columns
        for col in df.columns:
            if col not in column_mapping:
                if strict:
                    logger.warning(f"  Dropping unmapped column: '{col}'")
                else:
                    # Keep unmapped columns with normalized names
                    normalized = cls.normalize_column_name(col)
                    column_mapping[col] = normalized
                    unmapped_columns.append(col)
                    logger.debug(f"  Keeping unmapped column: '{col}' -> '{normalized}'")
        
        # Apply the mapping
        df = df.rename(columns=column_mapping)
        
        # Log summary
        mapped_count = len(column_mapping) - len(unmapped_columns)
        logger.info(f"✅ Column standardization complete:")
        logger.info(f"   - Mapped to BigQuery: {mapped_count}")
        logger.info(f"   - Unmapped/kept: {len(unmapped_columns)}")
        
        return df
    
    @staticmethod
    def normalize_column_name(name: str) -> str:
        """
        Normalize a column name to a safe format.
        Used for columns that don't map to BigQuery schema.
        """
        # Convert to string and strip
        name = str(name).strip()
        
        # Replace spaces and special chars with underscore
        name = re.sub(r'[\s\-\.]+', '_', name)
        name = re.sub(r'[^\w]', '', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('_')
        
        # Convert to lowercase
        name = name.lower()
        
        # Handle empty result
        if not name:
            name = 'column'
        
        return name
    
    @classmethod
    def ensure_required_columns(
        cls,
        df: pd.DataFrame,
        fill_missing: bool = True
    ) -> pd.DataFrame:
        """
        Ensure all required BigQuery columns exist.
        
        Args:
            df: DataFrame to check
            fill_missing: If True, add missing columns with default values
            
        Returns:
            DataFrame with all required columns
        """
        missing_columns = []
        
        for col in cls.BIGQUERY_COLUMNS:
            if col not in df.columns:
                missing_columns.append(col)
                
                if fill_missing:
                    # Add column with appropriate default value
                    if col in cls.MONEY_COLUMNS:
                        df[col] = 0.0
                    elif col in cls.INTEGER_COLUMNS:
                        df[col] = 0
                    elif col in cls.DATETIME_COLUMNS:
                        df[col] = pd.NaT
                    else:
                        df[col] = ''
        
        if missing_columns:
            logger.info(f"Added {len(missing_columns)} missing columns: {missing_columns}")
        
        return df
    
    @classmethod
    def reorder_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reorder columns to match BigQuery schema order.
        """
        # Get columns that exist in both DataFrame and schema
        existing_bq_cols = [col for col in cls.BIGQUERY_COLUMNS if col in df.columns]
        
        # Get any extra columns not in schema
        extra_cols = [col for col in df.columns if col not in cls.BIGQUERY_COLUMNS]
        
        # Combine in order: BigQuery columns first, then extras
        ordered_columns = existing_bq_cols + extra_cols
        
        return df[ordered_columns]
    
    @classmethod
    def validate_schema(
        cls,
        df: pd.DataFrame,
        raise_on_error: bool = False
    ) -> tuple[bool, List[str]]:
        """
        Validate that DataFrame matches expected schema.
        
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Check for required columns
        required_cols = [
            'Message', 'sending_time', 'Price', 'Sent', 'Viewed'
        ]
        
        for col in required_cols:
            if col not in df.columns:
                issues.append(f"Missing required column: {col}")
        
        # Check data types
        if 'sending_time' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['sending_time']):
                issues.append("Column 'sending_time' is not datetime type")
        
        for col in cls.MONEY_COLUMNS:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append(f"Column '{col}' is not numeric type")
        
        for col in cls.INTEGER_COLUMNS:
            if col in df.columns:
                if not pd.api.types.is_integer_dtype(df[col]) and not pd.api.types.is_float_dtype(df[col]):
                    issues.append(f"Column '{col}' is not numeric type")
        
        is_valid = len(issues) == 0
        
        if not is_valid and raise_on_error:
            raise ValueError(f"Schema validation failed: {'; '.join(issues)}")
        
        return is_valid, issues


def clean_model_name(name: Any) -> str:
    """
    Clean model/page names to be consistent.
    'Tessa Thomas VIP' -> 'tessathomasvip'
    """
    if not name or pd.isna(name):
        return ""
    
    # Convert to string, lowercase, remove spaces
    cleaned = str(name).lower().strip()
    
    # Remove file extensions if present
    cleaned = re.sub(r'\.(xlsx|xls|csv)$', '', cleaned)
    
    # Remove common suffixes
    suffixes = ['vip', 'paid', 'free', 'page', 'both']
    for suffix in suffixes:
        cleaned = re.sub(f'\\s*{suffix}$', '', cleaned, flags=re.IGNORECASE)
    
    # Remove all spaces and special characters
    cleaned = re.sub(r'\s+', '', cleaned)
    cleaned = re.sub(r'[^\w\-_]', '', cleaned)
    
    return cleaned


def standardize_dataframe(
    df: pd.DataFrame,
    strict: bool = False,
    fill_missing: bool = True,
    reorder: bool = True
) -> pd.DataFrame:
    """
    Complete standardization pipeline for a DataFrame.
    
    Args:
        df: DataFrame to standardize
        strict: Only keep columns that map to BigQuery schema
        fill_missing: Add missing required columns
        reorder: Reorder columns to match schema
        
    Returns:
        Standardized DataFrame
    """
    if df.empty:
        return df
    
    standardizer = ColumnStandardizer()
    
    # Step 1: Standardize column names
    df = standardizer.standardize_columns(df, strict=strict)
    
    # Step 2: Ensure required columns exist
    if fill_missing:
        df = standardizer.ensure_required_columns(df)
    
    # Step 3: Reorder columns
    if reorder:
        df = standardizer.reorder_columns(df)
    
    # Step 4: Clean model names if present
    if 'page_name' in df.columns:
        df['page_name'] = df['page_name'].apply(clean_model_name)
    
    # Step 5: Validate
    is_valid, issues = standardizer.validate_schema(df)
    if not is_valid:
        logger.warning(f"Schema validation issues: {issues}")
    
    return df


def get_column_report(df: pd.DataFrame) -> str:
    """
    Generate a report of column mappings.
    """
    standardizer = ColumnStandardizer()
    report_lines = ["Column Mapping Report", "=" * 50]
    
    for col in df.columns:
        mapped = False
        for bq_name, variations in standardizer.COLUMN_MAPPINGS.items():
            if col in variations or col.lower() in [v.lower() for v in variations]:
                report_lines.append(f"✅ '{col}' -> '{bq_name}'")
                mapped = True
                break
        
        if not mapped:
            normalized = standardizer.normalize_column_name(col)
            report_lines.append(f"❓ '{col}' -> '{normalized}' (unmapped)")
    
    report_lines.append("=" * 50)
    return "\n".join(report_lines)


# Testing
if __name__ == "__main__":
    print("Column Standardization Test Suite")
    print("=" * 60)
    
    # Test with sample DataFrame
    test_df = pd.DataFrame({
        'Message': ['Test message 1', 'Test message 2'],
        'Sending time': ['Aug 22, 2025 at 11:07 PM', 'Aug 23, 2025 at 10:00 AM'],
        'sender': ['User1', 'User2'],
        'price': ['$10.00', '$20.00'],
        'sent': [100, 200],
        'random_column': ['x', 'y']
    })
    
    print("\nOriginal columns:", list(test_df.columns))
    
    # Standardize
    standardized = standardize_dataframe(test_df)
    
    print("\nStandardized columns:", list(standardized.columns))
    
    # Get report
    print("\n" + get_column_report(test_df))
    
    # Test model name cleaning
    print("\nModel Name Cleaning Tests:")
    test_names = [
        'Tessa Thomas VIP',
        'Diana Grace Paid',
        'Titty Talia Free Page',
        'Sarah Rose.xlsx'
    ]
    
    for name in test_names:
        cleaned = clean_model_name(name)
        print(f"  '{name}' -> '{cleaned}'")
    
    print("\n✅ Standardization module ready!")