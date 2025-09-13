# BigQuery Cleanup List - Post Migration
**Date:** September 11, 2025  
**Migration Status:** ✅ COMPLETE

## Summary
We've successfully migrated from the old Gmail ETL architecture to the new layered architecture. Here's what can be safely removed:

---

## 🗑️ TABLES TO DELETE

### `staging` dataset (OLD architecture)
These tables are from the old system and have been replaced:

1. **`staging.gmail_etl_daily`** ⚠️ WAIT
   - Status: Active old table (keep for 1 week as safety)
   - Replacement: `layer_02_staging.gmail_events_staging`
   - Action: DELETE after confirming new ETL runs successfully for 1 week

2. **`staging.gmail_etl_daily_old_20250909_215137`** ✅ DELETE NOW
   - Status: Old backup from Sept 9
   - Action: Safe to delete immediately

3. **`staging.gmail_etl_daily_legacy_20250911`** ⚠️ KEEP
   - Status: Today's backup before migration
   - Action: Keep for 30 days as migration backup

4. **`staging.historical_message_staging`** ❓ REVIEW
   - Status: Unknown usage
   - Action: Check if used by any scheduled queries first

5. **`staging.historical_message_staging_old_20250909_215137`** ✅ DELETE NOW
   - Status: Old backup from Sept 9
   - Action: Safe to delete immediately

6. **`staging.creator_stats_upload`** ❓ REVIEW
   - Status: Unknown if part of Gmail ETL
   - Action: Verify usage before deletion

### `raw` dataset backups
These are old backups that can be removed:

1. **`raw.caption_library_backup_20250909`** ✅ DELETE NOW
2. **`raw.model_profiles_enhanced_backup_20250909`** ✅ DELETE NOW
3. **`raw.scheduled_sends_backup_20250909`** ✅ DELETE NOW
4. **`raw.username_mapping_backup_20250909`** ✅ DELETE NOW

---

## 🔍 VIEWS TO UPDATE OR DELETE

### `staging` dataset views
These views reference the old `staging.gmail_etl_daily` table:

1. **`staging.v_gmail_etl_daily_deduped`** ⚠️ UPDATE
   - Currently points to: `staging.gmail_etl_daily`
   - Action: Update to point to new table OR delete if unused

2. **`staging.gmail_etl_normalized`** ✅ KEEP
   - Status: Already updated to use new architecture
   - Points to: `layer_02_staging.fn_gmail_events_normalized`

3. **`staging.v_all_historical_enhanced`** ❓ REVIEW
   - Check if references old tables
   
4. **`staging.v_historical_filtered`** ❓ REVIEW
   - Check if references old tables

5. **`staging.creator_stats_latest`** ❓ REVIEW
   - Check if part of Gmail ETL pipeline

6. **`staging.creator_stats_norm`** ❓ REVIEW
   - Check if part of Gmail ETL pipeline

---

## 🔧 TABLE FUNCTIONS TO REVIEW

### Old function in wrong location
1. **`staging.fn_gmail_etl_normalized`** ✅ DELETE
   - Status: Old location
   - Replacement: `layer_02_staging.fn_gmail_events_normalized`
   - Action: Delete after confirming nothing references it

---

## 📅 SCHEDULED QUERIES TO UPDATE

Check these scheduled queries for references to old tables:
1. **`core_message_facts_hourly`** - May reference old staging
2. **`core_caption_bank_autoupdate_12h`** - May use old caption extraction

---

## 🚀 SAFE CLEANUP SCRIPT

```bash
#!/bin/bash
# BigQuery Cleanup Script - SAFE DELETIONS ONLY
# Run this to remove confirmed obsolete objects

echo "Starting BigQuery cleanup..."

# Delete old backups from Sept 9
echo "Removing Sept 9 backups..."
bq rm -f -t staging.gmail_etl_daily_old_20250909_215137
bq rm -f -t staging.historical_message_staging_old_20250909_215137
bq rm -f -t raw.caption_library_backup_20250909
bq rm -f -t raw.model_profiles_enhanced_backup_20250909
bq rm -f -t raw.scheduled_sends_backup_20250909
bq rm -f -t raw.username_mapping_backup_20250909

# Delete old table function in wrong location
echo "Removing old table function..."
bq rm -f --routine staging.fn_gmail_etl_normalized

echo "✅ Safe cleanup complete!"
echo ""
echo "⚠️ MANUAL REVIEW REQUIRED for:"
echo "- staging.gmail_etl_daily (wait 1 week)"
echo "- staging.gmail_etl_daily_legacy_20250911 (keep 30 days)"
echo "- Views that might reference old tables"
echo "- Scheduled queries"
```

---

## 📋 VERIFICATION CHECKLIST

Before running full cleanup:
- [ ] New Python ETL successfully writes to `layer_02_staging.gmail_events_staging`
- [ ] Table function `layer_02_staging.fn_gmail_events_normalized` works correctly
- [ ] Fact table `layer_03_foundation.fact_message_send` populates correctly
- [ ] No scheduled queries reference `staging.gmail_etl_daily`
- [ ] No views critically depend on old tables
- [ ] ML pipelines updated to use new architecture

---

## 🎯 NEW ARCHITECTURE COMPONENTS (KEEP)

✅ **KEEP ALL OF THESE:**
- `layer_02_staging.gmail_events_staging` - New staging table
- `layer_02_staging.fn_gmail_events_normalized` - Table function
- `layer_02_staging.fn_gmail_events_last_n_days` - Convenience wrapper
- `layer_03_foundation.fact_message_send` - Fact table
- `layer_03_foundation.sp_upsert_fact_gmail_message_send` - MERGE procedure
- `ops.quarantine_gmail` - Quarantine table
- `ops.v_caption_ingestion_monitor` - Monitoring view
- `staging.gmail_etl_normalized` - Backward compatibility view

---

## 📝 NOTES
- The migration from `staging.gmail_etl_daily` (12,693 rows) to `layer_02_staging.gmail_events_staging` is complete
- No data needed quarantining during migration (all dates parsed successfully)
- The old table `staging.gmail_etl_daily` should be kept for 1 week to ensure smooth transition
- Today's backup `staging.gmail_etl_daily_legacy_20250911` should be kept for 30 days