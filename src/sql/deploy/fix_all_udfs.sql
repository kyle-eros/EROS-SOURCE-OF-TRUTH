-- Fixed UDFs without problematic Unicode escapes

-- util.canonicalize_caption: Simpler version
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.canonicalize_caption`(text STRING)
RETURNS STRING AS (
  REGEXP_REPLACE(
    NORMALIZE(LOWER(TRIM(COALESCE(text,''))), NFKC),
    r'\s+', ' '  -- Just collapse whitespace
  )
);

-- util.emoji_count: Simple emoji detection
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.emoji_count`(text STRING)
RETURNS INT64 AS (
  COALESCE(
    ARRAY_LENGTH(
      REGEXP_EXTRACT_ALL(COALESCE(text,''), r'[😀-🙏🌀-🗿💀-📿🚀-🛿]')
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