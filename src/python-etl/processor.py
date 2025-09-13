# src/processor.py
"""
Excel file processor and data transformer.
Handles reading Excel files, normalizing columns, and cleaning data.
"""

import re
import logging
from typing import Optional, Dict, Any, List, Union, Tuple
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

from exceptions import ExcelProcessingError, DataValidationError

logger = logging.getLogger(__name__)


class ExcelProcessor:
    """
    Comprehensive Excel file processor for infloww.com reports.
    """
    
    # Expected columns in infloww.com reports (for validation)
    EXPECTED_COLUMNS = {
        'message': ['Message', 'message', 'Caption', 'caption', 'Text', 'text'],
        'sending_time': ['Sending_time', 'Sending time', 'SendingTime', 'Date', 'Timestamp'],
        'sender': ['Sender', 'sender', 'Scheduler', 'scheduler', 'Sent by'],
        'status': ['Status', 'status', 'State', 'state'],
        'price': ['Price', 'price', 'Amount', 'Cost'],
        'sent': ['Sent', 'sent', 'Sent count', 'Messages sent'],
        'viewed': ['Viewed', 'viewed', 'Views', 'View count'],
        'purchased': ['Purchased', 'purchased', 'Purchases', 'Buy count'],
        'earnings': ['Earnings', 'earnings', 'Revenue', 'Total'],
        'withdrawn_by': ['Withdrawn_by', 'Withdrawn by', 'WithdrawnBy']
    }
    
    @staticmethod
    def excel_to_dataframe(
        file_path: str,
        sheet_name: Optional[Union[str, int]] = None,
        validate: bool = True
    ) -> pd.DataFrame:
        """
        Read an Excel file into a pandas DataFrame with validation.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Specific sheet to read (default: first sheet)
            validate: Whether to validate the DataFrame structure
            
        Returns:
            Processed DataFrame
            
        Raises:
            ExcelProcessingError: If file cannot be read or validated
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ExcelProcessingError(str(file_path), "File does not exist")
        
        if not file_path.suffix.lower() in ['.xlsx', '.xls']:
            raise ExcelProcessingError(str(file_path), f"Invalid file type: {file_path.suffix}")
        
        logger.info(f"Reading Excel file: {file_path.name}")
        
        try:
            # Try reading with multiple attempts for potentially corrupted files
            df = None
            engines = ['openpyxl', 'xlrd']
            
            for engine in engines:
                try:
                    if sheet_name is not None:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
                    else:
                        # Try reading all sheets if first sheet fails
                        try:
                            df = pd.read_excel(file_path, engine=engine)
                        except Exception:
                            # Try reading all sheets and take first non-empty one
                            xl_file = pd.ExcelFile(file_path, engine=engine)
                            for sheet in xl_file.sheet_names:
                                temp_df = pd.read_excel(file_path, sheet_name=sheet, engine=engine)
                                if not temp_df.empty:
                                    df = temp_df
                                    logger.info(f"  Using sheet: {sheet}")
                                    break
                    
                    if df is not None and not df.empty:
                        break
                        
                except Exception as e:
                    logger.debug(f"  Failed with {engine}: {e}")
                    continue
            
            if df is None:
                raise ExcelProcessingError(str(file_path), "Could not read Excel file with any engine")
            
            logger.info(f"  Read {len(df)} rows, {len(df.columns)} columns")
            
            # Log column names for debugging
            logger.debug(f"  Columns found: {list(df.columns)[:10]}...")
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Remove completely empty columns
            df = df.dropna(axis=1, how='all')
            
            # Check again after cleaning
            if df.empty:
                logger.warning(f"DataFrame is empty after cleaning for {file_path}")
                # Return a minimal DataFrame with expected columns instead of raising error
                df = pd.DataFrame({
                    'message': ['[Empty Report]'],
                    'sending_time': [pd.Timestamp.now()],
                    'sender': [''],
                    'status': ['empty'],
                    'price': [0],
                    'sent': [0],
                    'viewed': [0],
                    'purchased': [0],
                    'earnings': [0],
                    'withdrawn_by': ['']
                })
                logger.info("  Created placeholder row for empty report")
            
            if validate:
                ExcelProcessor._validate_dataframe(df, str(file_path))
            
            return df
            
        except pd.errors.EmptyDataError:
            raise ExcelProcessingError(str(file_path), "Excel file contains no data")
        except Exception as e:
            if isinstance(e, ExcelProcessingError):
                raise
            raise ExcelProcessingError(str(file_path), f"Failed to read Excel: {e}")
    
    @staticmethod
    def _validate_dataframe(df: pd.DataFrame, file_path: str):
        """Validate that DataFrame has expected structure."""
        # Check for minimum expected columns
        found_columns = {
            'message': False,
            'sending_time': False,
            'price': False
        }
        
        df_columns_lower = [col.lower() for col in df.columns]
        
        for expected_type, variations in ExcelProcessor.EXPECTED_COLUMNS.items():
            for variation in variations:
                if variation.lower() in df_columns_lower:
                    if expected_type in found_columns:
                        found_columns[expected_type] = True
                    break
        
        # At minimum, we need message and sending_time
        if not found_columns['message']:
            logger.warning(f"No message column found in {file_path}")
        
        if not found_columns['sending_time']:
            logger.warning(f"No sending_time column found in {file_path}")
    
    @staticmethod
    def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame column names for consistency.
        - Lowercase
        - Replace spaces with underscores
        - Remove special characters
        - Remove duplicate underscores
        """
        def normalize(col: str) -> str:
            # Convert to string and lowercase
            col = str(col).strip().lower()
            
            # Replace common separators with underscore
            col = re.sub(r'[\s\-\.]+', '_', col)
            
            # Remove special characters except underscore
            col = re.sub(r'[^\w]', '', col)
            
            # Remove duplicate underscores
            col = re.sub(r'_+', '_', col)
            
            # Remove leading/trailing underscores
            col = col.strip('_')
            
            # Handle empty result
            if not col:
                col = 'column'
            
            return col
        
        original_columns = list(df.columns)
        normalized_columns = [normalize(c) for c in df.columns]
        
        # Handle duplicate column names
        seen = {}
        for i, col in enumerate(normalized_columns):
            if col in seen:
                seen[col] += 1
                normalized_columns[i] = f"{col}_{seen[col]}"
            else:
                seen[col] = 1
        
        df.columns = normalized_columns
        
        # Log column mapping for debugging
        for orig, norm in zip(original_columns, normalized_columns):
            if orig != norm:
                logger.debug(f"  Column renamed: '{orig}' -> '{norm}'")
        
        return df
    
    @staticmethod
    def parse_datetime_column(
        series: pd.Series,
        formats: Optional[List[str]] = None
    ) -> pd.Series:
        """
        Parse datetime column with multiple format attempts.
        
        Args:
            series: Pandas Series with datetime strings
            formats: List of datetime formats to try
            
        Returns:
            Series with parsed datetime values
        """
        if formats is None:
            formats = [
                '%b %d, %Y at %I:%M %p',  # "Aug 22, 2025 at 11:07 PM"
                '%b %d, %Y %I:%M %p',      # "Aug 22, 2025 11:07 PM"
                '%Y-%m-%d %H:%M:%S',       # "2025-08-22 23:07:00"
                '%m/%d/%Y %I:%M %p',       # "08/22/2025 11:07 PM"
                '%d/%m/%Y %H:%M',          # "22/08/2025 23:07"
            ]
        
        # First, try pandas automatic parsing
        result = pd.to_datetime(series, errors='coerce')
        
        # Count how many failed
        failed_count = result.isna().sum()
        
        if failed_count > 0 and failed_count < len(series):
            # Try specific formats for failed values
            failed_mask = result.isna()
            failed_values = series[failed_mask]
            
            for fmt in formats:
                if failed_values.empty:
                    break
                    
                try:
                    # Remove 'at' if present in format
                    cleaned = failed_values.astype(str).str.replace(' at ', ' ', regex=False)
                    parsed = pd.to_datetime(cleaned, format=fmt, errors='coerce')
                    
                    # Update successful parses
                    valid_parsed = ~parsed.isna()
                    if valid_parsed.any():
                        result.loc[failed_values[valid_parsed].index] = parsed[valid_parsed]
                        failed_values = failed_values[~valid_parsed]
                        logger.debug(f"  Parsed {valid_parsed.sum()} dates with format: {fmt}")
                except:
                    continue
        
        # Log any remaining failures
        final_failed = result.isna().sum()
        if final_failed > 0:
            logger.warning(f"  Could not parse {final_failed} datetime values")
        
        return result
    
    @staticmethod
    def clean_money_column(series: pd.Series) -> pd.Series:
        """
        Clean money/currency columns.
        Removes currency symbols, commas, and converts to float.
        """
        # Convert to string first
        series = series.astype(str)
        
        # Remove currency symbols and whitespace
        series = series.str.replace(r'[$£€¥₹]', '', regex=True)
        series = series.str.replace(',', '', regex=False)
        series = series.str.strip()
        
        # Handle special cases
        series = series.replace(['', '-', 'N/A', 'nan', 'None'], '0')
        
        # Handle parentheses for negative values
        def handle_parentheses(val):
            if '(' in val and ')' in val:
                val = val.replace('(', '-').replace(')', '')
            return val
        
        series = series.apply(handle_parentheses)
        
        # Convert to numeric
        return pd.to_numeric(series, errors='coerce').fillna(0.0)
    
    @staticmethod
    def clean_integer_column(series: pd.Series) -> pd.Series:
        """
        Clean integer columns.
        Removes commas and converts to integer.
        """
        # Convert to string first
        series = series.astype(str)
        
        # Remove commas and whitespace
        series = series.str.replace(',', '', regex=False)
        series = series.str.strip()
        
        # Handle special cases
        series = series.replace(['', '-', 'N/A', 'nan', 'None'], '0')
        
        # Convert to numeric (use nullable integer)
        return pd.to_numeric(series, errors='coerce').fillna(0).astype('Int64')
    
    @staticmethod
    def clean_text_column(series: pd.Series, remove_html: bool = False) -> pd.Series:
        """
        Clean text columns.
        Removes extra whitespace, handles encoding issues.
        """
        # Convert to string
        series = series.astype(str)
        
        # Replace common null values
        series = series.replace(['nan', 'None', 'null', 'N/A'], '')
        
        # Remove HTML if requested
        if remove_html:
            series = series.apply(lambda x: re.sub(r'<[^>]+>', '', x))
        
        # Clean whitespace
        series = series.str.strip()
        series = series.apply(lambda x: ' '.join(x.split()))
        
        # Remove non-printable characters
        series = series.apply(lambda x: ''.join(char for char in x if char.isprintable() or char in '\n\t'))
        
        return series
    
    @staticmethod
    def process_infloww_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Process infloww.com report DataFrame with all standard transformations.
        """
        logger.info("Processing infloww.com report DataFrame")
        
        # 1. Normalize headers
        df = ExcelProcessor.normalize_headers(df)
        
        # 2. Parse datetime columns
        datetime_cols = ['sending_time', 'sendingtime', 'date', 'timestamp']
        for col in datetime_cols:
            if col in df.columns:
                logger.debug(f"  Parsing datetime column: {col}")
                df[col] = ExcelProcessor.parse_datetime_column(df[col])
        
        # 3. Clean money columns
        money_cols = ['price', 'earnings', 'revenue', 'total', 'amount', 'cost']
        for col in money_cols:
            if col in df.columns:
                logger.debug(f"  Cleaning money column: {col}")
                df[col] = ExcelProcessor.clean_money_column(df[col])
        
        # 4. Clean integer columns
        int_cols = ['sent', 'viewed', 'purchased', 'sent_count', 'view_count']
        for col in int_cols:
            if col in df.columns:
                logger.debug(f"  Cleaning integer column: {col}")
                df[col] = ExcelProcessor.clean_integer_column(df[col])
        
        # 5. Clean text columns
        text_cols = ['message', 'sender', 'status', 'withdrawn_by', 'caption']
        for col in text_cols:
            if col in df.columns:
                logger.debug(f"  Cleaning text column: {col}")
                df[col] = ExcelProcessor.clean_text_column(df[col])
        
        # 6. Remove completely empty rows that may have been created
        df = df.replace('', np.nan)
        df = df.dropna(how='all')
        df = df.replace(np.nan, '')
        
        logger.info(f"  Final shape: {df.shape[0]} rows, {df.shape[1]} columns")
        
        return df


# Convenience functions for backward compatibility
def excel_to_dataframe(file_path: str) -> pd.DataFrame:
    """Read an Excel file into a pandas DataFrame."""
    return ExcelProcessor.excel_to_dataframe(file_path)


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names for BigQuery."""
    return ExcelProcessor.normalize_headers(df)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all standard cleaning operations to a DataFrame."""
    return ExcelProcessor.process_infloww_dataframe(df)


def get_dataframe_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get a summary of the DataFrame for logging/debugging.
    """
    summary = {
        'rows': len(df),
        'columns': len(df.columns),
        'column_names': list(df.columns),
        'dtypes': df.dtypes.to_dict(),
        'null_counts': df.isnull().sum().to_dict(),
        'memory_usage': df.memory_usage(deep=True).sum() / 1024 / 1024,  # MB
    }
    
    # Add sample of first few rows (as dict)
    if not df.empty:
        summary['sample'] = df.head(3).to_dict('records')
    
    return summary


def validate_required_columns(
    df: pd.DataFrame,
    required: List[str],
    raise_on_missing: bool = True
) -> Tuple[bool, List[str]]:
    """
    Validate that DataFrame has required columns.
    
    Args:
        df: DataFrame to validate
        required: List of required column names
        raise_on_missing: Whether to raise exception if columns missing
        
    Returns:
        Tuple of (all_present: bool, missing: List[str])
    """
    df_columns_lower = [col.lower() for col in df.columns]
    missing = []
    
    for req_col in required:
        if req_col.lower() not in df_columns_lower:
            missing.append(req_col)
    
    if missing and raise_on_missing:
        raise DataValidationError(
            field_name="columns",
            issue=f"Missing required columns: {missing}",
            sample_value=str(list(df.columns)[:10])
        )
    
    return len(missing) == 0, missing


# Testing
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        print(f"Testing processor with file: {test_file}")
        
        try:
            # Read and process
            df = excel_to_dataframe(test_file)
            df = clean_dataframe(df)
            
            # Get summary
            summary = get_dataframe_summary(df)
            
            print("\nDataFrame Summary:")
            print(f"  Rows: {summary['rows']}")
            print(f"  Columns: {summary['columns']}")
            print(f"  Memory: {summary['memory_usage']:.2f} MB")
            print(f"\nColumns: {summary['column_names']}")
            
            # Validate
            required = ['message', 'sending_time', 'price']
            valid, missing = validate_required_columns(df, required, raise_on_missing=False)
            
            if valid:
                print(f"\n✅ All required columns present")
            else:
                print(f"\n⚠️ Missing columns: {missing}")
                
        except Exception as e:
            print(f"\n❌ Error: {e}")
            sys.exit(1)
    else:
        print("Excel Processor Module")
        print("Usage: python processor.py <excel_file>")