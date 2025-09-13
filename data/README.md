# OnlyFans Caption Classification System

## Overview
This system processes OnlyFans captions and classifies them into business categories while ensuring TOS compliance.

## Directory Structure

### 📁 input_data/
Original source data files:
- `ALLTIME_MASS_MESSAGE_STATS - MASS.MESSAGE.STATS(has data for inactive girls).csv`
- `EROS CAPTION LOGGER 💙 - BUMPS.csv`
- `EROS CAPTION LOGGER 💙 - PPVS (2).csv`

### 📁 output_data/
Final TOS compliant classified captions:
- `captions_MM_PPV_tos_compliant.csv` - 23,802 paid content captions (price > $0)
- `captions_MM_FollowUp_Bump_tos_compliant.csv` - 13,388 follow-up messages (price = $0, no renew/link/tip)
- `captions_Renew_Campaign_tos_compliant.csv` - 262 renewal campaign messages
- `captions_Tip_Campaign_tos_compliant.csv` - 213 tip request messages
- `captions_Link_Drop_tos_compliant.csv` - 157 traffic-driving links
- `captions_category_counts_tos_compliant.csv` - Category summary statistics
- `captions_clean_categorized_tos_compliant.csv` - Master file with all categorized captions

### 📁 audit_logs/
Compliance and quality assurance files:
- `removed_for_tos_violations.csv` - 138 captions removed for TOS violations

## Business Classification Rules

1. **MassMessage PPV**: All captions with price > $0
2. **Follow Up MassMessage Bump**: Price = $0 + no renew/link/tip mentions
3. **Renew Campaign**: Contains "RENEW"/"renew" or renew links
4. **Tip Campaign**: Specifically directs fans to tip
5. **Link Drop**: Contains https links (excluding renew links)

## TOS Compliance

All output files are 100% OnlyFans TOS compliant. Removed content includes:
- Familial/incest content (87 captions)
- Parental familial content (39 captions)
- Underage indicators (6 captions)
- Non-consensual content (6 captions)

**Stepmom Allowlist**: Role-play "stepmom" content is preserved as fantasy content.

## Processing Script

`tos_compliant_business_rules_classifier.py` - Final working script that:
- Loads and processes all input data
- Applies TOS compliance filtering
- Removes duplicates
- Classifies using business rules
- Generates all output files

## Final Statistics

- **Total processed**: 64,863 captions
- **TOS violations removed**: 138 captions
- **Duplicates removed**: 26,903 captions
- **Final clean captions**: 37,822 captions

## Usage

To reprocess the data:
```bash
python tos_compliant_business_rules_classifier.py
```

All output files will be regenerated in the current directory and can then be moved to the appropriate folders.