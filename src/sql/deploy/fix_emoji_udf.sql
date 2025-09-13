-- Fix emoji UDF with proper regex pattern

-- util.emoji_count: Fixed emoji detection
CREATE OR REPLACE FUNCTION `of-scheduler-proj.util.emoji_count`(text STRING)
RETURNS INT64 AS (
  COALESCE(
    ARRAY_LENGTH(
      REGEXP_EXTRACT_ALL(
        COALESCE(text,''), 
        r'[😀-🙏🌀-🗿💀-📿🚀-🛿🇀-🇿✂️-➿⚠️-⚿🤍-🥿🦀-🦯🧀-🧿🩰-🩳🪀-🪶⭐🌟✨💫]'
      )
    ), 0
  )
);