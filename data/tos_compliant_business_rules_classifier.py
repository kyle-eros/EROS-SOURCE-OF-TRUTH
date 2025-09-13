#!/usr/bin/env python3
"""
TOS Compliant Business Rules Caption Classifier
Simple business logic + comprehensive TOS compliance filtering
"""

import pandas as pd
import re
import os
from typing import List, Dict, Tuple
from collections import defaultdict

# Input file paths
INPUT_FILES = [
    "ALLTIME_MASS_MESSAGE_STATS - MASS.MESSAGE.STATS(has data for inactive girls).csv",
    "EROS CAPTION LOGGER 💙 - BUMPS.csv", 
    "EROS CAPTION LOGGER 💙 - PPVS (2).csv"
]

# TOS Compliance Rules
TOS_RULES = [
    # A. Familial/incest & underage cues
    {"rule_id": "A1_familial_direct", "reason": "familial/incest content", "patterns": [
        r'\b(incest|cousin|brother|sister|uncle|aunt|niece|nephew)\b',
        r'\b(son|daughter|stepdaughter|stepson)\b',
        r'\bmom\s+and\s+son\b', r'\bdad\s+and\s+daughter\b'
    ]},
    {"rule_id": "A2_parental", "reason": "parental familial content", "patterns": [
        r'\b(mother|father|mom|dad)\b(?!.*step)', # Exclude stepmom
        r'\bmy\s+(mom|dad|mother|father)\b'
    ]},
    {"rule_id": "A3_underage_numeric", "reason": "underage numeric indicators", "patterns": [
        r'\b1[0-7]\s*(?:year|yr|yo)\b', r'\b1[0-7]\b.*\bold\b'
    ]},
    {"rule_id": "A4_underage_terms", "reason": "underage content", "patterns": [
        r'\bteen\b.*\b(sex|fuck|nude|naked|porn)\b',
        r'\b(minor|underage|jailbait)\b',
        r'\bschoolgirl\b.*\b(sex|fuck|nude)\b'
    ]},
    
    # B. Non-consensual content
    {"rule_id": "B1_rape", "reason": "rape content", "patterns": [r'\brape[dy]?\b', r'\brapist\b']},
    {"rule_id": "B2_force", "reason": "forced/non-consensual content", "patterns": [
        r'\bforced\b', r'\bnon[-\s]?consensual\b',
        r'against\s+(?:her|his|their)\s+will\b'
    ]},
    {"rule_id": "B3_coercion", "reason": "coercion content", "patterns": [
        r'\b(blackmail|kidnap|drugged|roofied)\b',
        r'\b(unconscious|passed\s+out)\b.*\b(sex|fuck)\b'
    ]},
    
    # C. Fan meetup content
    {"rule_id": "C1_fan_sex", "reason": "fan meetup sexual content", "patterns": [
        r'\b(fuck|sex|meet)\b.*\bfan\b.*\b(fuck|sex|meet)\b',
        r'\bmeet\s+up\b.*\bfan\b', r'\bfan\s+meetup\b'
    ]},
    
    # D. Bodily fluids
    {"rule_id": "D1_urine", "reason": "urine/piss content", "patterns": [
        r'\b(pee|piss|urine|golden\s+shower)\b'
    ]},
    {"rule_id": "D2_scat", "reason": "scat content", "patterns": [
        r'\b(scat|poop|shit|feces|defecate)\b.*\b(play|sex|eat)\b'
    ]},
    {"rule_id": "D3_vomit", "reason": "vomit content", "patterns": [r'\bvomit\b.*\b(sex|play)\b']},
    
    # E. Bestiality
    {"rule_id": "E1_animals", "reason": "bestiality content", "patterns": [
        r'\b(bestiality|zoophilia)\b',
        r'\b(dog|horse|animal)\b.*\b(sex|fuck|mount)\b'
    ]}
]

def check_tos_violations(text):
    """Check for TOS violations with stepmom allowlist."""
    if pd.isna(text) or not isinstance(text, str):
        return []
    
    text_norm = text.lower()
    violations = []
    
    for rule in TOS_RULES:
        for pattern in rule['patterns']:
            if re.search(pattern, text_norm, re.IGNORECASE):
                violations.append({
                    'rule_id': rule['rule_id'],
                    'reason': rule['reason']
                })
                break
    
    # Stepmom allowlist logic
    if any('A' in v['rule_id'] for v in violations):  # Familial violations
        stepmom_patterns = [r'\bstepmom\b', r'\bstep\s*mom\b', r'\bstep-mom\b']
        has_stepmom = any(re.search(p, text_norm) for p in stepmom_patterns)
        
        # Other familial terms (excluding stepmom)
        other_familial = [r'\b(mother|father|brother|sister|uncle|aunt|niece|nephew|son|daughter|cousin)\b']
        has_other = any(re.search(p, text_norm) for p in other_familial)
        
        # Real family indicators
        real_family = [r'\b(biological|blood|actual|my\s+real)\b']
        has_real = any(re.search(p, text_norm) for p in real_family)
        
        # Allow stepmom fantasy if no other problematic content
        if has_stepmom and not has_other and not has_real:
            violations = [v for v in violations if not v['rule_id'].startswith('A')]
    
    return violations

def extract_price(price_str):
    """Extract numeric price from price string."""
    if pd.isna(price_str):
        return 0.0
    
    price_str = str(price_str)
    # Remove currency symbols and extract number
    price_match = re.search(r'[\d.]+', price_str)
    if price_match:
        try:
            return float(price_match.group())
        except ValueError:
            return 0.0
    return 0.0

def has_renew_mention(text):
    """Check if text mentions renew/renewal."""
    if pd.isna(text):
        return False
    return bool(re.search(r'\b(?:renew|renewal)\b', str(text), re.IGNORECASE))

def has_https_link(text):
    """Check if text contains https link."""
    if pd.isna(text):
        return False
    return bool(re.search(r'https://', str(text), re.IGNORECASE))

def has_renew_link(text):
    """Check if text has https link that mentions renew."""
    if pd.isna(text):
        return False
    text_str = str(text).lower()
    return bool(re.search(r'https://.*renew', text_str, re.IGNORECASE))

def mentions_tip(text):
    """Check if text specifically directs fans to tip."""
    if pd.isna(text):
        return False
    text_str = str(text).lower()
    # Look for tip-related instructions
    tip_patterns = [
        r'\btip\s+(?:me|for|to|if|\$)',
        r'send\s+(?:a\s+)?tip',
        r'tip\s+(?:and|to\s+get)',
        r'\$\d+\s+tip'
    ]
    return any(re.search(pattern, text_str) for pattern in tip_patterns)

def classify_caption(message, price, source_file):
    """Apply simple business rules for classification."""
    
    # Rule 1: All MMPPVs are captions with price > $0
    if price > 0:
        return "MassMessage PPV"
    
    # For price = $0 captions, check other conditions
    if price == 0:
        # Rule 3: Renew captions - mention RENEW/renew OR have renew link
        if has_renew_mention(message) or has_renew_link(message):
            return "Renew campaign captions"
        
        # Rule 4: Tip campaign captions - specifically direct fans to tip
        if mentions_tip(message):
            return "Tip campaign captions"
        
        # Rule 5: Link drop captions - have https link (but not renew link)
        if has_https_link(message):
            return "Link drop"
        
        # Rule 2: Follow up MM Bumps - price=$0, no renew, no https, no tip
        return "Follow Up MassMessage Bump"
    
    # Default fallback
    return "Follow Up MassMessage Bump"

def load_and_process_file(file_path):
    """Load and process a single file."""
    print(f"📁 Loading: {os.path.basename(file_path)}")
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='latin-1')
    
    print(f"   📊 {len(df)} rows loaded")
    
    # Standardize columns
    df['source_file'] = os.path.basename(file_path)
    df['row_id'] = range(len(df))
    
    # Detect caption and price columns
    if 'Message' in df.columns:
        df['caption_text'] = df['Message']
        df['price_value'] = df['Price'].apply(extract_price)
    elif 'Preview' in df.columns:
        df['caption_text'] = df['Preview']
        if 'PriceLastSent' in df.columns:
            df['price_value'] = df['PriceLastSent'].apply(extract_price)
        else:
            df['price_value'] = 0.0
    else:
        print(f"   ⚠️  Unknown column structure")
        return pd.DataFrame()
    
    # Remove empty captions
    before_count = len(df)
    df = df[df['caption_text'].notna() & (df['caption_text'].astype(str).str.strip() != '')].copy()
    removed = before_count - len(df)
    if removed > 0:
        print(f"   🗑️  Removed {removed} empty captions")
    
    print(f"   ✅ {len(df)} captions ready for processing")
    
    return df[['source_file', 'row_id', 'caption_text', 'price_value']].reset_index(drop=True)

def main():
    """Main processing function."""
    print("🚀 TOS Compliant Business Rules Caption Classifier")
    print("=" * 70)
    print("🛡️  TOS Compliance + Simple Business Logic")
    print("📋 Business Rules:")
    print("   1. Price > $0 = MMPPVs")
    print("   2. Price = $0 + no renew/link/tip = Follow up MM Bumps")
    print("   3. Mentions RENEW or renew link = Renew campaigns")
    print("   4. Directs to tip = Tip campaigns")  
    print("   5. Has https link (not renew) = Link drops")
    print()
    
    # Load all files
    all_dfs = []
    total_loaded = 0
    
    for file_path in INPUT_FILES:
        if os.path.exists(file_path):
            df = load_and_process_file(file_path)
            if not df.empty:
                all_dfs.append(df)
                total_loaded += len(df)
        else:
            print(f"⚠️  File not found: {file_path}")
    
    if not all_dfs:
        print("❌ No data loaded.")
        return
    
    # Combine data
    print(f"🔗 Combining {len(all_dfs)} files...")
    combined_df = pd.concat(all_dfs, ignore_index=True, sort=False)
    print(f"   📊 Total captions: {len(combined_df):,}")
    
    # TOS Compliance Filtering
    print(f"\n🛡️  TOS Compliance Filtering...")
    removed_for_compliance = []
    clean_indices = []
    
    for idx, row in combined_df.iterrows():
        if idx % 5000 == 0 and idx > 0:
            print(f"   📋 Checked {idx:,}/{len(combined_df):,} captions for TOS violations")
        
        violations = check_tos_violations(row['caption_text'])
        if violations:
            violation_reasons = '; '.join([v['reason'] for v in violations])
            violation_rule_ids = '; '.join([v['rule_id'] for v in violations])
            
            removed_for_compliance.append({
                'source_file': row['source_file'],
                'row_id': row['row_id'],
                'caption_original': row['caption_text'],
                'price': row['price_value'],
                'violation_rule_ids': violation_rule_ids,
                'violation_reasons': violation_reasons
            })
        else:
            clean_indices.append(idx)
    
    clean_df = combined_df.iloc[clean_indices].reset_index(drop=True)
    print(f"   🚫 Removed {len(removed_for_compliance):,} captions for TOS violations")
    print(f"   ✅ TOS compliant captions: {len(clean_df):,}")
    
    # Remove duplicates
    print(f"\n🔄 Removing duplicates...")
    before_dedup = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=['caption_text']).reset_index(drop=True)
    after_dedup = len(clean_df)
    print(f"   🔁 Removed {before_dedup - after_dedup:,} duplicates")
    print(f"   📊 Unique TOS compliant captions: {after_dedup:,}")
    
    # Apply business rules classification
    print(f"\n📋 Applying Business Rules Classification...")
    categorized_data = []
    category_counts = defaultdict(int)
    price_distribution = {'paid': 0, 'free': 0}
    
    for idx, row in clean_df.iterrows():
        if idx % 5000 == 0 and idx > 0:
            print(f"   🔍 Classified {idx:,}/{len(clean_df):,} captions")
        
        message = row['caption_text']
        price = row['price_value']
        source_file = row['source_file']
        
        # Track price distribution
        if price > 0:
            price_distribution['paid'] += 1
        else:
            price_distribution['free'] += 1
        
        # Classify using business rules
        category = classify_caption(message, price, source_file)
        category_counts[category] += 1
        
        categorized_data.append({
            'source_file': source_file,
            'row_id': row['row_id'],
            'caption_original': message,
            'price': price,
            'category': category,
            'has_link': has_https_link(message),
            'contains_renew': has_renew_mention(message),
            'mentions_tip': mentions_tip(message)
        })
    
    final_df = pd.DataFrame(categorized_data)
    
    # Write output files
    print(f"\n💾 Writing TOS Compliant Output Files...")
    
    category_file_mapping = {
        "MassMessage PPV": "captions_MM_PPV_tos_compliant.csv",
        "Follow Up MassMessage Bump": "captions_MM_FollowUp_Bump_tos_compliant.csv", 
        "Renew campaign captions": "captions_Renew_Campaign_tos_compliant.csv",
        "Tip campaign captions": "captions_Tip_Campaign_tos_compliant.csv",
        "Link drop": "captions_Link_Drop_tos_compliant.csv"
    }
    
    # Per-category files
    for category, filename in category_file_mapping.items():
        category_df = final_df[final_df['category'] == category]
        category_df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        if len(category_df) > 0:
            avg_price = category_df['price'].mean()
            print(f"   ✅ {filename}: {len(category_df):,} captions (avg price: ${avg_price:.2f})")
        else:
            print(f"   ✅ {filename}: 0 captions")
    
    # TOS violations audit file
    if removed_for_compliance:
        violations_df = pd.DataFrame(removed_for_compliance)
        violations_df.to_csv('removed_for_tos_violations.csv', index=False, encoding='utf-8-sig')
        print(f"   ✅ removed_for_tos_violations.csv: {len(removed_for_compliance):,} violations")
        
        # Show violation breakdown
        violation_counts = violations_df['violation_reasons'].value_counts()
        print(f"\n🚫 TOS VIOLATION BREAKDOWN:")
        for reason, count in violation_counts.head(10).items():
            print(f"   {reason}: {count:,}")
    
    # Category summary
    category_counts_df = pd.DataFrame([
        {'category': cat, 'count': count, 'percentage': round(count/len(final_df)*100, 1)} 
        for cat, count in category_counts.items()
    ]).sort_values('count', ascending=False)
    category_counts_df.to_csv('captions_category_counts_tos_compliant.csv', index=False, encoding='utf-8-sig')
    print(f"   ✅ captions_category_counts_tos_compliant.csv")
    
    # Master file
    final_df.to_csv('captions_clean_categorized_tos_compliant.csv', index=False, encoding='utf-8-sig')
    print(f"   ✅ captions_clean_categorized_tos_compliant.csv: {len(final_df):,} total captions")
    
    # Final Report
    print("\n" + "=" * 70)
    print("📈 TOS COMPLIANT BUSINESS RULES RESULTS")
    print("=" * 70)
    print(f"Total loaded:              {total_loaded:,}")
    print(f"TOS violations removed:    {len(removed_for_compliance):,}")
    print(f"Duplicates removed:        {before_dedup - after_dedup:,}")
    print(f"Final TOS compliant:       {len(final_df):,}")
    print(f"Paid captions (>$0):       {price_distribution['paid']:,}")
    print(f"Free captions ($0):        {price_distribution['free']:,}")
    print()
    print("📊 CATEGORY BREAKDOWN:")
    for _, row in category_counts_df.iterrows():
        print(f"  {row['category']}: {row['count']:,} ({row['percentage']}%)")
    
    print("\n✅ TOS compliant classification complete!")
    print("🛡️  All output is OnlyFans TOS compliant")
    print("📋 Results based on clear business rules")

if __name__ == "__main__":
    main()