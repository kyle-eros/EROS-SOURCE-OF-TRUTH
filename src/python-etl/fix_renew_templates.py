#!/usr/bin/env python3
import pandas as pd
import re
from pathlib import Path

def fix_renew_templates(file_path):
    """Fix renew campaign templates to remove parentheses from username placeholder."""
    print(f"Processing {file_path.name}...")
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Fix the renew template format
    fixed_count = 0
    for idx, row in df.iterrows():
        caption = row['caption_original']
        
        if isinstance(caption, str) and '(ENTERUSERNAME)' in caption:
            # Replace (ENTERUSERNAME) with ENTERUSERNAME
            fixed_caption = caption.replace('(ENTERUSERNAME)', 'ENTERUSERNAME')
            df.at[idx, 'caption_original'] = fixed_caption
            fixed_count += 1
    
    # Save the updated file
    if fixed_count > 0:
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"Fixed {fixed_count} renew templates in {file_path.name}")
    else:
        print(f"No renew templates to fix in {file_path.name}")
    
    return fixed_count

def main():
    output_data_dir = Path("/Users/kylemerriman/Desktop/EROS-SYSTEM-SOURCE-OF-TRUTH/data/output_data")
    
    # Find all CSV files that contain captions (exclude the counts file)
    caption_files = []
    for file in output_data_dir.glob("*.csv"):
        if "category_counts" not in file.name:
            caption_files.append(file)
    
    print(f"Fixing renew templates in {len(caption_files)} files:")
    print()
    
    total_fixed = 0
    
    for file_path in caption_files:
        if file_path.exists():
            fixed = fix_renew_templates(file_path)
            total_fixed += fixed
        else:
            print(f"File not found: {file_path}")
    
    print()
    print(f"Total renew templates fixed: {total_fixed}")

if __name__ == "__main__":
    main()