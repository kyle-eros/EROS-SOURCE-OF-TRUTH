# src/hash_generator.py
"""
Hash generation for message deduplication and row uniqueness.
Generates two types of hashes:
1. caption_hash: For tracking unique messages/captions
2. row_key_v1: For ensuring row-level uniqueness
"""

import hashlib
import re
import logging
from typing import Optional, List, Dict, Any, Set
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class HashGenerator:
    """
    Centralized hash generation for the ETL pipeline.
    """
    
    # Column name mappings for different Excel formats
    MESSAGE_COLUMNS = [
        'Message', 'message',
        'Caption', 'caption',
        'Text', 'text',
        'Content', 'content',
        'caption_text', 'message_text'
    ]
    
    PAGE_COLUMNS = [
        'page_name', 'Page Name', 'PageName',
        'Page', 'page',
        'Username', 'username',
        'Creator', 'creator',
        'Model', 'model',
        'model_name'
    ]
    
    TIME_COLUMNS = [
        'sending_time', 'Sending_time', 'SendingTime',
        'Sending time',
        'Date', 'date',
        'Timestamp', 'timestamp',
        'scheduled_datetime'
    ]
    
    PRICE_COLUMNS = [
        'Price', 'price',
        'Amount', 'amount',
        'Cost', 'cost',
        'Total', 'total',
        'Revenue', 'revenue',
        'Earnings', 'earnings'
    ]
    
    @staticmethod
    def find_column(df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """
        Find a column in the dataframe from a list of possible names.
        Case-insensitive matching.
        """
        if df.empty or not possible_names:
            return None
        
        # First try exact match
        for col in df.columns:
            if col in possible_names:
                return col
        
        # Then try case-insensitive match
        df_cols_lower = {col.lower(): col for col in df.columns}
        for possible in possible_names:
            if possible.lower() in df_cols_lower:
                return df_cols_lower[possible.lower()]
        
        return None
    
    @staticmethod
    def clean_text_for_hash(text: Any) -> str:
        """
        Clean and normalize text for consistent hashing.
        """
        if pd.isna(text) or text is None:
            return ""
        
        # Convert to string
        text_str = str(text).strip()
        
        # Remove common noise
        if text_str.lower() in ['nan', 'none', 'null', 'n/a', '-', '--', '']:
            return ""
        
        # Normalize whitespace
        text_str = ' '.join(text_str.split())
        
        # Convert to lowercase for consistency
        text_str = text_str.lower()
        
        return text_str
    
    @staticmethod
    def generate_caption_hash(message: Any) -> str:
        """
        Generate deterministic hash for message content.
        Used for tracking caption performance and initial deduplication.
        
        Args:
            message: The message/caption text
            
        Returns:
            SHA-256 hash of the normalized message
        """
        cleaned = HashGenerator.clean_text_for_hash(message)
        
        if not cleaned:
            # Return consistent hash for empty messages
            # This allows us to track "empty" messages as a category
            return hashlib.sha256(b"__empty_message__").hexdigest()
        
        # Generate deterministic hash
        return hashlib.sha256(cleaned.encode('utf-8')).hexdigest()
    
    @staticmethod
    def generate_row_key(
        page_name: Any,
        message: Any,
        sending_time: Any,
        price: Any = None,
        additional_fields: Dict[str, Any] = None
    ) -> str:
        """
        Generate unique row key for deduplication.
        Combines multiple fields to ensure row uniqueness.
        
        Args:
            page_name: Page/model name
            message: Message content
            sending_time: When the message was sent
            price: Price of the message
            additional_fields: Extra fields to include in the hash
            
        Returns:
            SHA-256 hash of the composite key
        """
        # Clean all inputs
        clean_page = HashGenerator.clean_text_for_hash(page_name) or "unknown"
        clean_message = HashGenerator.clean_text_for_hash(message) or "empty"
        
        # Handle sending_time (could be string or datetime)
        if pd.isna(sending_time) or sending_time is None:
            clean_time = "unknown"
        elif isinstance(sending_time, (datetime, pd.Timestamp)):
            clean_time = sending_time.isoformat()
        else:
            clean_time = str(sending_time).strip() or "unknown"
        
        # Handle price
        if pd.isna(price) or price is None:
            clean_price = "0"
        else:
            try:
                # Round to 2 decimal places for consistency
                clean_price = f"{float(price):.2f}"
            except:
                clean_price = "0"
        
        # Build composite key
        composite_parts = [
            clean_page,
            clean_message,
            clean_time,
            clean_price
        ]
        
        # Add any additional fields
        if additional_fields:
            for key in sorted(additional_fields.keys()):
                value = HashGenerator.clean_text_for_hash(additional_fields[key])
                if value:
                    composite_parts.append(f"{key}:{value}")
        
        composite_key = "|".join(composite_parts)
        
        # Generate hash
        return hashlib.sha256(composite_key.encode('utf-8')).hexdigest()
    
    @staticmethod
    def normalize_caption(message: Any) -> str:
        """
        Normalize message caption for analysis.
        Removes HTML, standardizes whitespace, etc.
        """
        cleaned = HashGenerator.clean_text_for_hash(message)
        
        if not cleaned:
            return ""
        
        # Remove HTML tags
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        
        # Remove URLs
        cleaned = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', cleaned)
        
        # Remove emojis and special characters
        cleaned = re.sub(r'[^\w\s\'".,!?-]', ' ', cleaned)
        
        # Normalize whitespace again
        cleaned = ' '.join(cleaned.split())
        
        return cleaned.strip()


def add_hash_columns(
    df: pd.DataFrame,
    page_name_override: Optional[str] = None,
    skip_existing: bool = True
) -> pd.DataFrame:
    """
    Add caption_hash, row_key_v1, and caption_norm columns to dataframe.
    
    Args:
        df: DataFrame to process
        page_name_override: Use this page name for all rows
        skip_existing: Skip generating hashes if columns already exist
        
    Returns:
        DataFrame with hash columns added
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to add_hash_columns")
        df['caption_hash'] = []
        df['row_key_v1'] = []
        df['caption_norm'] = []
        return df
    
    generator = HashGenerator()
    rows_count = len(df)
    logger.info(f"Adding hash columns to {rows_count} rows")
    
    # FIXED: Initialize skip_caption_hash at the beginning
    skip_caption_hash = False
    
    # Check if columns already exist
    if skip_existing:
        existing_cols = []
        if 'caption_hash' in df.columns:
            existing_cols.append('caption_hash')
        if 'row_key_v1' in df.columns:
            existing_cols.append('row_key_v1')
        if 'caption_norm' in df.columns:
            existing_cols.append('caption_norm')
        
        if existing_cols:
            logger.info(f"Skipping existing columns: {existing_cols}")
            # Only generate missing columns
            if 'caption_hash' in existing_cols:
                skip_caption_hash = True
    
    # Find relevant columns
    message_col = generator.find_column(df, generator.MESSAGE_COLUMNS)
    page_col = generator.find_column(df, generator.PAGE_COLUMNS) if not page_name_override else None
    time_col = generator.find_column(df, generator.TIME_COLUMNS)
    price_col = generator.find_column(df, generator.PRICE_COLUMNS)
    
    logger.info(f"Column mapping - Message: {message_col}, Page: {page_col}, Time: {time_col}, Price: {price_col}")
    
    # Generate caption hash and normalized caption
    if message_col and not skip_caption_hash:
        logger.debug("Generating caption hashes...")
        df['caption_hash'] = df[message_col].apply(generator.generate_caption_hash)
        df['caption_norm'] = df[message_col].apply(generator.normalize_caption)
    elif not skip_caption_hash:
        logger.warning("No message column found - using placeholder hashes")
        df['caption_hash'] = hashlib.sha256(b"__no_message_column__").hexdigest()
        df['caption_norm'] = ""
    
    # Generate row keys
    logger.debug("Generating row keys...")
    
    def generate_row_key_for_row(row):
        """Generate row key for a single row"""
        page_value = page_name_override if page_name_override else (row.get(page_col, '') if page_col else '')
        message_value = row.get(message_col, '') if message_col else ''
        time_value = row.get(time_col, '') if time_col else ''
        price_value = row.get(price_col, None) if price_col else None
        
        return generator.generate_row_key(
            page_value,
            message_value,
            time_value,
            price_value
        )
    
    if 'row_key_v1' not in df.columns or not skip_existing:
        df['row_key_v1'] = df.apply(generate_row_key_for_row, axis=1)
    
    logger.info(f"✅ Successfully added hash columns to {rows_count} rows")
    
    return df


def find_duplicates(df: pd.DataFrame, key_column: str = 'row_key_v1') -> pd.DataFrame:
    """
    Find duplicate rows based on a key column.
    
    Args:
        df: DataFrame to check
        key_column: Column to use for duplicate detection
        
    Returns:
        DataFrame containing only the duplicate rows
    """
    if df.empty or key_column not in df.columns:
        return pd.DataFrame()
    
    # Find duplicates
    duplicated_mask = df.duplicated(subset=[key_column], keep=False)
    duplicates = df[duplicated_mask].copy()
    
    if not duplicates.empty:
        # Sort by the key column for easier review
        duplicates = duplicates.sort_values(by=key_column)
        logger.warning(f"Found {len(duplicates)} duplicate rows based on {key_column}")
    
    return duplicates


def remove_duplicates(
    df: pd.DataFrame,
    key_column: str = 'row_key_v1',
    keep: str = 'first'
) -> pd.DataFrame:
    """
    Remove duplicate rows based on a key column.
    
    Args:
        df: DataFrame to deduplicate
        key_column: Column to use for duplicate detection
        keep: Which duplicate to keep ('first', 'last', False)
        
    Returns:
        DataFrame with duplicates removed
    """
    if df.empty or key_column not in df.columns:
        return df
    
    original_count = len(df)
    df_deduped = df.drop_duplicates(subset=[key_column], keep=keep)
    removed_count = original_count - len(df_deduped)
    
    if removed_count > 0:
        logger.info(f"Removed {removed_count} duplicate rows (kept {keep})")
    
    return df_deduped


def get_hash_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get statistics about hash columns in the DataFrame.
    
    Returns:
        Dictionary with statistics
    """
    stats = {
        'total_rows': len(df),
        'has_caption_hash': 'caption_hash' in df.columns,
        'has_row_key': 'row_key_v1' in df.columns,
        'has_caption_norm': 'caption_norm' in df.columns
    }
    
    if 'caption_hash' in df.columns:
        unique_captions = df['caption_hash'].nunique()
        stats['unique_captions'] = unique_captions
        stats['duplicate_captions'] = len(df) - unique_captions
        stats['empty_captions'] = (df['caption_hash'] == hashlib.sha256(b"__empty_message__").hexdigest()).sum()
    
    if 'row_key_v1' in df.columns:
        unique_rows = df['row_key_v1'].nunique()
        stats['unique_rows'] = unique_rows
        stats['duplicate_rows'] = len(df) - unique_rows
    
    return stats


# Testing functions
if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("Hash Generator Test Suite")
    print("=" * 60)
    
    # Test basic hash generation
    print("\n1. Testing basic hash generation:")
    test_messages = [
        "I love cumming at the same time...",
        "  I LOVE cumming at the same time...  ",  # Should produce same hash
        "",
        None,
        "Check out my new content! 🔥"
    ]
    
    for msg in test_messages:
        hash_val = HashGenerator.generate_caption_hash(msg)
        norm_val = HashGenerator.normalize_caption(msg)
        print(f"  Message: {repr(msg)[:50]}")
        print(f"  Hash: {hash_val}")
        print(f"  Normalized: {repr(norm_val)[:50]}")
        print()
    
    # Test with sample DataFrame
    print("\n2. Testing with sample DataFrame:")
    sample_df = pd.DataFrame({
        'Message': ['Hello world', 'Hello world', 'Different message', None, ''],
        'Page': ['Titty Talia Paid', 'Diana Grace', 'Titty Talia Paid', 'Unknown', 'Test'],
        'Sending_time': ['2025-01-01 12:00:00'] * 5,
        'Price': [10.0, 10.0, 20.0, 0, 5.5]
    })
    
    result_df = add_hash_columns(sample_df)
    print(f"  Columns added: {[col for col in result_df.columns if col not in sample_df.columns]}")
    
    # Check for duplicates
    duplicates = find_duplicates(result_df)
    print(f"  Duplicates found: {len(duplicates)}")
    
    # Get statistics
    stats = get_hash_statistics(result_df)
    print(f"  Statistics: {stats}")
    
    print("\n✅ Hash generator tests completed!")