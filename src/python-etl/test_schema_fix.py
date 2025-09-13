#!/usr/bin/env python3
"""
Test script to verify BigQuery schema fix for Gmail ETL pipeline
"""

import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import os

# Set up BigQuery client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '')
client = bigquery.Client(project='of-scheduler-proj')

def test_schema():
    """Test that the schema matches what we expect"""
    table_id = "of-scheduler-proj.staging.gmail_etl_daily"
    table = client.get_table(table_id)
    
    print("Current BigQuery Table Schema:")
    print("=" * 50)
    
    expected_columns = {
        'Message': 'STRING',
        'Sending_time': 'STRING', 
        'Sender': 'STRING',
        'Status': 'STRING',
        'Price': 'STRING',
        'Sent': 'INTEGER',
        'Viewed': 'INTEGER',
        'Purchased': 'INTEGER',
        'Earnings': 'FLOAT',
        'Withdrawn_by': 'STRING',
        'message_id': 'STRING',
        'source_file': 'STRING'
    }
    
    actual_columns = {}
    for field in table.schema:
        actual_columns[field.name] = field.field_type
        print(f"  {field.name}: {field.field_type}")
    
    print("\nValidation Results:")
    print("=" * 50)
    
    missing_columns = []
    type_mismatches = []
    
    for col_name, expected_type in expected_columns.items():
        if col_name not in actual_columns:
            missing_columns.append(col_name)
            print(f"❌ Missing column: {col_name}")
        elif actual_columns[col_name] != expected_type:
            type_mismatches.append(f"{col_name}: expected {expected_type}, got {actual_columns[col_name]}")
            print(f"⚠️  Type mismatch - {col_name}: expected {expected_type}, got {actual_columns[col_name]}")
        else:
            print(f"✅ {col_name}: {expected_type}")
    
    if not missing_columns and not type_mismatches:
        print("\n✨ Schema validation PASSED! All required columns present with correct types.")
        return True
    else:
        print("\n❌ Schema validation FAILED!")
        if missing_columns:
            print(f"   Missing columns: {', '.join(missing_columns)}")
        if type_mismatches:
            print(f"   Type mismatches: {', '.join(type_mismatches)}")
        return False

def test_insert():
    """Test inserting a sample row with the new schema"""
    print("\nTesting data insertion:")
    print("=" * 50)
    
    # Create a sample DataFrame matching the schema
    sample_data = pd.DataFrame([{
        'Message': 'Test message from schema fix test',
        'Sending_time': 'Sep 11, 2025 at 10:00 AM',
        'Sender': 'test_sender',
        'Status': 'test',
        'Price': '$5.00',
        'Sent': 1,
        'Viewed': 1,
        'Purchased': 0,
        'Earnings': 5.0,
        'Withdrawn_by': 'test_user',
        'message_id': 'test_msg_123',
        'source_file': 'gs://test/test.xlsx'
    }])
    
    try:
        table_id = "of-scheduler-proj.staging.gmail_etl_daily"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=False
        )
        
        job = client.load_table_from_dataframe(
            sample_data, 
            table_id, 
            job_config=job_config
        )
        job.result()  # Wait for the job to complete
        
        print("✅ Test data inserted successfully!")
        print(f"   Inserted 1 test row with message_id='test_msg_123'")
        
        # Query to verify
        query = f"""
        SELECT message_id, Message, Sender 
        FROM `{table_id}`
        WHERE message_id = 'test_msg_123'
        LIMIT 1
        """
        result = client.query(query).result()
        for row in result:
            print(f"   Verified: message_id={row.message_id}, Message={row.Message[:30]}...")
        
        # Clean up test data
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE message_id = 'test_msg_123'
        """
        client.query(delete_query).result()
        print("   Cleaned up test data")
        
        return True
        
    except Exception as e:
        print(f"❌ Test insertion failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Gmail ETL BigQuery Schema Fix Test")
    print("=" * 60)
    
    schema_ok = test_schema()
    
    if schema_ok:
        insert_ok = test_insert()
        
        if insert_ok:
            print("\n" + "=" * 60)
            print("✨ ALL TESTS PASSED! The schema fix is working correctly.")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print("⚠️  Schema is correct but insertion test failed.")
            print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ Schema validation failed. Please check the table structure.")
        print("=" * 60)