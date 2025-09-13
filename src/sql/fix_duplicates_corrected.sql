-- =====================================================
-- FIX DUPLICATE ACTIVE OVERRIDES (CORRECTED)
-- Remove duplicate entries keeping the most recent
-- =====================================================

-- Create temp table with duplicates to remove
CREATE TEMP TABLE duplicate_entries AS
WITH ranked_overrides AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY username_std 
      ORDER BY updated_at DESC NULLS LAST
    ) as rn
  FROM `of-scheduler-proj.core.active_overrides`
)
SELECT username_std, include, note, updated_at
FROM ranked_overrides 
WHERE rn > 1;

-- Show what we're about to delete
SELECT 
  'DUPLICATES TO BE REMOVED' as action,
  COUNT(*) as records_to_delete,
  COUNT(DISTINCT username_std) as creators_affected
FROM duplicate_entries;

-- Delete the duplicates (keeping the most recent entry for each creator)
DELETE FROM `of-scheduler-proj.core.active_overrides`
WHERE (username_std, IFNULL(updated_at, TIMESTAMP('1900-01-01'))) IN (
  SELECT username_std, IFNULL(updated_at, TIMESTAMP('1900-01-01'))
  FROM duplicate_entries
);

-- Verify results
SELECT 
  'AFTER CLEANUP' as status,
  COUNT(*) as total_records,
  COUNT(DISTINCT username_std) as unique_creators,
  COUNT(*) - COUNT(DISTINCT username_std) as remaining_duplicates
FROM `of-scheduler-proj.core.active_overrides`;