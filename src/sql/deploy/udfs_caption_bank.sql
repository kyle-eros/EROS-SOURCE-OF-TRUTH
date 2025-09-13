-- File: $OUT_DIR/udfs_caption_bank.sql
-- Utility functions for automated caption bank processing

-- util.canonicalize_caption: Lowercase, trim, collapse whitespace, strip zero-widths, normalize Unicode
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.canonicalize_caption`(text STRING)
RETURNS STRING AS (
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      NORMALIZE(LOWER(TRIM(COALESCE(text,''))), NFKC),
      r'[\u200B-\u200F\uFEFF\u202A-\u202E]', ''  -- Remove zero-width chars
    ),
    r'\s+', ' '  -- Collapse whitespace
  )
);

-- util.caption_hash_v2: Canonical hash generation
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.caption_hash_v2`(text STRING)
RETURNS STRING AS (
  TO_HEX(SHA256(`of-scheduler-proj.util.canonicalize_caption`(text)))
);

-- util.length_bin: Word count categorization
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.length_bin`(text STRING)
RETURNS STRING AS (
  CASE
    WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(COALESCE(text,''), r'\b[\p{L}\p{N}]+\b')) < 12 THEN 'short'
    WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(COALESCE(text,''), r'\b[\p{L}\p{N}]+\b')) <= 24 THEN 'med'
    ELSE 'long'
  END
);

-- util.emoji_count: Comprehensive emoji detection
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.emoji_count`(text STRING)
RETURNS INT64 AS (
  COALESCE(
    ARRAY_LENGTH(
      REGEXP_EXTRACT_ALL(
        COALESCE(text,''), 
        r'[\x{1F300}-\x{1F9FF}]|[\x{2600}-\x{26FF}]|[\x{2700}-\x{27BF}]|[\x{1F000}-\x{1F02F}]|[\x{1F0A0}-\x{1F0FF}]|[\x{1F100}-\x{1F1FF}]'
      )
    ), 0
  )
);

-- util.emoji_bin: Categorize emoji density
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.emoji_bin`(text STRING)
RETURNS STRING AS (
  CASE
    WHEN `of-scheduler-proj.util.emoji_count`(text) = 0 THEN 'none'
    WHEN `of-scheduler-proj.util.emoji_count`(text) <= 2 THEN 'low'
    WHEN `of-scheduler-proj.util.emoji_count`(text) <= 5 THEN 'medium'
    ELSE 'high'
  END
);

-- util.has_cta: Detect call-to-action patterns
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.has_cta`(text STRING)
RETURNS BOOL AS (
  REGEXP_CONTAINS(
    LOWER(COALESCE(text,'')), 
    r'\b(click|tap|swipe|join|subscribe|follow|buy|get|claim|unlock|watch|see|check out|sign up|register|download|install|start|try|discover|learn more|shop now|order|book|reserve|dm|message|open|tip|bundle|sale|special|discount)\b'
  )
);

-- util.has_urgency: Detect urgency indicators
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.has_urgency`(text STRING)
RETURNS BOOL AS (
  REGEXP_CONTAINS(
    LOWER(COALESCE(text,'')), 
    r'\b(now|today|tonight|hurry|quick|fast|limited|exclusive|ending|expires|last chance|don\'t miss|only|final|urgent|immediately|asap|right now|act fast|while supplies last)\b|[!]{2,}'
  )
);

-- util.ends_with_question: Check for question ending
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.ends_with_question`(text STRING)
RETURNS BOOL AS (
  REGEXP_CONTAINS(TRIM(COALESCE(text,'')), r'\?\s*$')
);

-- util.detect_explicitness: Enhanced explicitness detection with fallback heuristics
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.detect_explicitness`(text STRING)
RETURNS STRING AS (
  CASE
    -- Explicit content markers
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(text,'')), r'\b(xxx|nsfw|adult|explicit|nude|naked|sex|fuck|pussy|cock|dick|cum|orgasm|masturbat|porn)\b') THEN 'explicit'
    -- Moderate content markers
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(text,'')), r'\b(sexy|hot|naughty|tease|seduc|flirt|kiss|touch|body|curves|lingerie|underwear|bra|panties|shower|bed|bedroom|intimate)\b') THEN 'moderate'
    -- Commercial/promotional content is often moderate
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(text,'')), r'\b(buy|unlock|tip|bundle|sale|special|discount|exclusive|vip|premium|ppv|pay per view)\b') THEN 'moderate'
    -- Mild/friendly content markers
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(text,'')), r'\b(cute|sweet|hello|hi|good morning|good night|love|heart|beautiful|gorgeous|amazing|wonderful)\b') THEN 'mild'
    -- Default for unclassified content
    ELSE 'pending_review'
  END
);

-- util.compute_theme_tags: Multi-tag support with comma separation
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.compute_theme_tags`(text STRING)
RETURNS STRING AS (
  CASE
    WHEN text IS NULL OR TRIM(text) = '' THEN 'untagged'
    ELSE (
      SELECT ARRAY_TO_STRING(
        ARRAY(
          SELECT DISTINCT tag FROM (
            SELECT 'birthday' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'\b(birthday|bday|cake|candle|celebration|party)\b')
            UNION ALL
            SELECT 'romantic' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'\b(love|heart|romance|valentine|kiss|cuddle|darling|sweetheart|date|romantic)\b')
            UNION ALL
            SELECT 'motivational' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'\b(motivat|inspir|success|achieve|dream|goal|believe|strong|confident|power|positiv)\b')
            UNION ALL
            SELECT 'seasonal' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'\b(christmas|halloween|thanksgiving|easter|summer|winter|spring|fall|holiday|xmas|nye|new year)\b')
            UNION ALL
            SELECT 'flirty' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'\b(flirt|tease|wink|cute|naughty|playful|cheeky)\b')
            UNION ALL
            SELECT 'greeting' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'^(hey|hi|hello|good morning|good night|gm|gn|what\'s up|how are you)\b')
            UNION ALL
            SELECT 'promotional' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'\b(sale|discount|offer|deal|promo|special|exclusive|limited|bundle|ppv)\b')
            UNION ALL
            SELECT 'question' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(text, r'\?\s*$')
            UNION ALL
            SELECT 'urgent' AS tag FROM (SELECT 1) WHERE REGEXP_CONTAINS(LOWER(text), r'\b(now|today|tonight|hurry|quick|limited|expires|last chance)\b|[!]{2,}')
          )
        ),
        ','
      )
    )
  END
);