-- =====================================
-- EROS USERNAME MAPPING MANAGER
-- =====================================
-- This script creates and maintains the global username mapping system
-- ensuring accurate cross-table joins for performance tracking

-- =====================================
-- 1. ANALYZE CURRENT MAPPING GAPS
-- =====================================
-- Check coverage across all main tables
WITH coverage_analysis AS (
  -- Caption Rank usernames (username_page format: "torirae__main") 
  SELECT 
    'caption_rank' as source_table,
    username_page as raw_username,
    REGEXP_EXTRACT(username_page, r'^([^_]+)') as base_extract,
    CASE 
      WHEN REGEXP_CONTAINS(username_page, r'__main$') THEN 'main'
      WHEN REGEXP_CONTAINS(username_page, r'__vip$') THEN 'vip' 
      WHEN REGEXP_CONTAINS(username_page, r'paid__') THEN 'paid'
      ELSE 'unknown'
    END as account_type_guess
  FROM `of-scheduler-proj.mart.caption_rank_next24_v3_tbl`
  WHERE DATE(slot_dt_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY username_page

  UNION ALL

  -- Message Facts usernames (username_std format: "tori rae")
  SELECT 
    'message_facts' as source_table,
    username_std as raw_username,
    username_std as base_extract,
    'message_account' as account_type_guess
  FROM `of-scheduler-proj.core.message_facts`
  WHERE DATE(sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY username_std
),
-- Check what's already mapped
current_mappings AS (
  SELECT raw_username, standard_username, account_type
  FROM `of-scheduler-proj.raw.username_mapping`
)

-- Show unmapped usernames that need attention
SELECT 
  ca.source_table,
  ca.raw_username,
  ca.base_extract,
  ca.account_type_guess,
  CASE WHEN cm.standard_username IS NOT NULL THEN 'MAPPED' ELSE 'NEEDS_MAPPING' END as status
FROM coverage_analysis ca
LEFT JOIN current_mappings cm ON ca.raw_username = cm.raw_username
WHERE cm.standard_username IS NULL
ORDER BY ca.source_table, ca.raw_username;

-- =====================================
-- 2. AUTO-POPULATE MISSING MAPPINGS  
-- =====================================
-- Insert missing caption_rank username mappings
INSERT INTO `of-scheduler-proj.raw.username_mapping` (
  mapping_id,
  raw_username,
  normalized_username, 
  base_username,
  account_type,
  standard_username,
  created_at,
  updated_at
)
WITH new_mappings AS (
  SELECT DISTINCT
    GENERATE_UUID() as mapping_id,
    cr.username_page as raw_username,
    
    -- Normalize username by removing underscores and suffixes
    LOWER(TRIM(REGEXP_REPLACE(
      REGEXP_EXTRACT(cr.username_page, r'^([^_]+)'),
      r'(paid|free)$', ''
    ))) as normalized_base,
    
    -- Extract base username  
    LOWER(TRIM(REGEXP_REPLACE(
      REGEXP_EXTRACT(cr.username_page, r'^([^_]+)'),
      r'(paid|free)$', ''
    ))) as base_username,
    
    -- Determine account type
    CASE 
      WHEN REGEXP_CONTAINS(cr.username_page, r'__main$') THEN 'main'
      WHEN REGEXP_CONTAINS(cr.username_page, r'__vip$') THEN 'vip'
      WHEN REGEXP_CONTAINS(cr.username_page, r'paid__') THEN 'paid'
      ELSE 'unknown'
    END as account_type,
    
    -- Create standardized username for joining
    -- Convert "torirae__main" -> "tori rae" format to match message_facts
    REGEXP_REPLACE(
      LOWER(TRIM(REGEXP_REPLACE(
        REGEXP_EXTRACT(cr.username_page, r'^([^_]+)'),
        r'(paid|free)$', ''
      ))),
      r'([a-z])([A-Z])', r'\1 \2'  -- Add space between camelCase
    ) as standard_username,
    
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
    
  FROM `of-scheduler-proj.mart.caption_rank_next24_v3_tbl` cr
  WHERE DATE(cr.slot_dt_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND cr.username_page NOT IN (
      SELECT raw_username FROM `of-scheduler-proj.raw.username_mapping`
    )
)

SELECT * FROM new_mappings
WHERE standard_username IS NOT NULL;

-- =====================================
-- 3. CREATE FUZZY MATCHING FUNCTION
-- =====================================
-- For cases where exact matching fails, create fuzzy matching
CREATE OR REPLACE FUNCTION `of-scheduler-proj.core.fuzzy_username_match`(
  input_username STRING, 
  candidate_username STRING
) RETURNS FLOAT64
LANGUAGE js AS """
  // Normalize usernames for comparison
  function normalize(str) {
    return str.toLowerCase()
             .replace(/[^a-z]/g, '') // Remove special chars
             .replace(/paid|free|vip|main/g, '') // Remove account type suffixes
             .trim();
  }
  
  const norm1 = normalize(input_username);
  const norm2 = normalize(candidate_username);
  
  // Exact match
  if (norm1 === norm2) return 1.0;
  
  // Levenshtein distance for similarity
  const matrix = [];
  for (let i = 0; i <= norm2.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= norm1.length; j++) {
    matrix[0][j] = j;
  }
  for (let i = 1; i <= norm2.length; i++) {
    for (let j = 1; j <= norm1.length; j++) {
      if (norm2.charAt(i - 1) === norm1.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1, // substitution
          matrix[i][j - 1] + 1,     // insertion
          matrix[i - 1][j] + 1      // deletion
        );
      }
    }
  }
  
  const distance = matrix[norm2.length][norm1.length];
  const similarity = 1 - (distance / Math.max(norm1.length, norm2.length));
  
  return similarity >= 0.8 ? similarity : 0; // Return 0 for low similarity
""";

-- =====================================
-- 4. CREATE MAPPING LOOKUP VIEW
-- =====================================
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_username_mapping_complete` AS
WITH 
-- Direct mappings from mapping table
direct_mappings AS (
  SELECT 
    raw_username,
    standard_username,
    account_type,
    'direct' as mapping_method,
    1.0 as confidence
  FROM `of-scheduler-proj.raw.username_mapping`
  WHERE standard_username IS NOT NULL
),
-- Fuzzy mappings for unmapped usernames
fuzzy_mappings AS (
  SELECT 
    cr.username_page as raw_username,
    mf.username_std as standard_username,
    CASE 
      WHEN REGEXP_CONTAINS(cr.username_page, r'__main$') THEN 'main'
      WHEN REGEXP_CONTAINS(cr.username_page, r'__vip$') THEN 'vip' 
      ELSE 'unknown'
    END as account_type,
    'fuzzy' as mapping_method,
    `of-scheduler-proj.core.fuzzy_username_match`(cr.username_page, mf.username_std) as confidence
  FROM (
    SELECT DISTINCT username_page 
    FROM `of-scheduler-proj.mart.caption_rank_next24_v3_tbl`
    WHERE DATE(slot_dt_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND username_page NOT IN (SELECT raw_username FROM `of-scheduler-proj.raw.username_mapping`)
  ) cr
  CROSS JOIN (
    SELECT DISTINCT username_std
    FROM `of-scheduler-proj.core.message_facts`
    WHERE DATE(sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  ) mf
  WHERE `of-scheduler-proj.core.fuzzy_username_match`(cr.username_page, mf.username_std) > 0.8
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cr.username_page ORDER BY confidence DESC) = 1
)

-- Combine all mappings
SELECT * FROM direct_mappings
UNION ALL 
SELECT * FROM fuzzy_mappings
WHERE raw_username NOT IN (SELECT raw_username FROM direct_mappings);

-- =====================================
-- 5. VALIDATION QUERY
-- =====================================
-- Test the complete mapping coverage
SELECT 
  'Final Coverage Test' as test_name,
  COUNT(DISTINCT cr.username_page) as total_caption_usernames,
  COUNT(DISTINCT vm.standard_username) as mapped_usernames,
  ROUND(COUNT(DISTINCT vm.standard_username) / COUNT(DISTINCT cr.username_page) * 100, 1) as coverage_pct
FROM `of-scheduler-proj.mart.caption_rank_next24_v3_tbl` cr
LEFT JOIN `of-scheduler-proj.core.v_username_mapping_complete` vm
  ON cr.username_page = vm.raw_username
WHERE DATE(cr.slot_dt_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);