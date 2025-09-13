-- =====================================================
-- EROS ENHANCED CAPTION SYSTEM - INTELLIGENT METRICS
-- Phase B: Data Quality & New Metrics
-- Project: of-scheduler-proj
-- =====================================================

-- Variables (edit these as needed)
DECLARE PROJECT_ID STRING DEFAULT "of-scheduler-proj";
DECLARE DATASETS STRUCT<raw STRING, core STRING, mart STRING> DEFAULT ("raw", "core", "mart");

-- =====================================================
-- 1) CAPTION SENTIMENT & ENERGY ANALYSIS
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_caption_sentiment_v1` AS
WITH f AS (
  SELECT
    caption_hash,
    caption_text,
    -- Urgency Score: Time-sensitive language
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(caption_text), r'(EXCLUSIVE|LIMITED|NOW|TODAY|TONIGHT|URGENT|HURRY|QUICK|FAST|EXPIRING|EXPIRES)') THEN 3
      WHEN REGEXP_CONTAINS(UPPER(caption_text), r'(SPECIAL|HOT|READY|WAIT|SOON|ALMOST|ALMOST|ENDING)') THEN 2 
      ELSE 1 
    END AS urgency_score,
    
    -- Intimacy Score: Personal connection language
    CASE 
      WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(daddy|baby|love|miss you|thinking of you|dream|imagine|personal|private|just for you)') THEN 3
      WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(babe|honey|darling|sweetie|cutie|special|close|together)') THEN 2 
      ELSE 1 
    END AS intimacy_score,
    
    -- Action Score: Call-to-action intensity
    CASE 
      WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(click|watch|see|check|look|unlock|open|view|download|get|grab)') THEN 3
      WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(want|need|ready|come|join|experience|enjoy|try)') THEN 2 
      ELSE 1 
    END AS action_score,
    
    -- Emoji Analysis
    ARRAY_LENGTH(REGEXP_EXTRACT_ALL(caption_text, r'[\p{Emoji_Presentation}\p{Extended_Pictographic}]')) AS emoji_count,
    LENGTH(caption_text) AS caption_length,
    
    -- Sexual Explicitness Indicators
    CASE
      WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(fuck|cock|pussy|ass|cum|dick|wet|hard|horny|sexy|nude|naked)') THEN 3
      WHEN REGEXP_CONTAINS(LOWER(caption_text), r'(tease|naughty|dirty|hot|play|touch|kiss|body)') THEN 2
      ELSE 1
    END AS sexual_intensity,
    
    -- Question/Engagement Pattern
    CASE
      WHEN REGEXP_CONTAINS(caption_text, r'\?') THEN 1
      ELSE 0
    END AS has_question
    
  FROM `of-scheduler-proj.raw.caption_library`
  WHERE caption_text IS NOT NULL
),
normalized AS (
  SELECT *,
    -- Composite Energy Score (0-1 scale)
    SAFE_DIVIDE(urgency_score + intimacy_score + action_score, 9.0) AS composite_energy,
    
    -- Emoji Density (emojis per 100 characters)
    SAFE_MULTIPLY(SAFE_DIVIDE(emoji_count, NULLIF(caption_length, 0)), 100) AS emoji_density_pct,
    
    -- Sexual intensity normalized
    SAFE_DIVIDE(sexual_intensity, 3.0) AS sexual_intensity_norm
    
  FROM f
)
SELECT 
  *,
  -- Final engagement prediction score
  (composite_energy * 0.4 + 
   LEAST(emoji_density_pct / 10.0, 1.0) * 0.2 + 
   sexual_intensity_norm * 0.3 + 
   has_question * 0.1) AS engagement_prediction_score
FROM normalized;

-- =====================================================
-- 2) PAGE ENGAGEMENT PATTERNS (Hour × Day Analysis)
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_page_engagement_patterns_v1` AS
WITH hourly AS (
  SELECT
    username_std,
    EXTRACT(HOUR FROM sending_ts) AS hour,
    EXTRACT(DAYOFWEEK FROM sending_ts) AS dow,
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS conversion_rate,
    AVG(price_usd) AS avg_price,
    AVG(earnings_usd) AS avg_earnings,
    STDDEV(earnings_usd) AS earnings_volatility,
    COUNT(*) AS n_messages,
    SUM(earnings_usd) AS total_earnings
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND sending_ts IS NOT NULL
    AND username_std IS NOT NULL
  GROUP BY 1, 2, 3
  HAVING n_messages >= 3  -- Minimum data for reliability
),
aggregated AS (
  SELECT
    username_std,
    -- Top conversion windows (hour, dow, rate)
    ARRAY_AGG(STRUCT(hour, dow, conversion_rate, avg_earnings, n_messages) 
              ORDER BY conversion_rate DESC LIMIT 10) AS top_conversion_windows,
    
    -- Revenue consistency (1 - CV where CV = std/mean)
    1 - SAFE_DIVIDE(
      SQRT(AVG(POW(earnings_volatility, 2))), 
      NULLIF(AVG(avg_earnings), 0)
    ) AS revenue_consistency,
    
    -- Schedule coverage (fraction of possible hour×dow slots used)
    SAFE_DIVIDE(COUNT(DISTINCT CONCAT(hour, '-', dow)), 168.0) AS schedule_coverage,
    
    -- Price elasticity (correlation between price and conversion)
    CORR(avg_price, conversion_rate) AS price_elasticity,
    
    -- Peak performance metrics
    MAX(conversion_rate) AS peak_conversion_rate,
    AVG(conversion_rate) AS avg_conversion_rate,
    MAX(avg_earnings) AS peak_earnings_per_message,
    SUM(total_earnings) AS total_90d_earnings,
    SUM(n_messages) AS total_90d_messages
    
  FROM hourly
  GROUP BY 1
)
SELECT
  *,
  -- Performance volatility score
  SAFE_DIVIDE(peak_conversion_rate - avg_conversion_rate, NULLIF(avg_conversion_rate, 0)) AS conversion_volatility,
  
  -- Message volume tier
  CASE
    WHEN total_90d_messages >= 1000 THEN 'HIGH_VOLUME'
    WHEN total_90d_messages >= 300 THEN 'MEDIUM_VOLUME'
    WHEN total_90d_messages >= 50 THEN 'LOW_VOLUME'
    ELSE 'MINIMAL_VOLUME'
  END AS volume_tier
FROM aggregated;

-- =====================================================
-- 3) CAPTION FATIGUE SCORING (30-day analysis)
-- =====================================================

CREATE OR REPLACE TABLE `of-scheduler-proj.core.caption_fatigue_scores_v1` AS
WITH usage_velocity AS (
  SELECT
    caption_hash,
    username_page,
    COUNT(*) AS times_used_30d,
    MAX(last_used_ts) AS most_recent_use,
    MIN(last_used_ts) AS first_use_30d,
    -- Usage dates for density calculation
    ARRAY_AGG(DISTINCT DATE(last_used_ts) ORDER BY DATE(last_used_ts)) AS usage_dates,
    -- Average days between uses
    SAFE_DIVIDE(
      DATE_DIFF(MAX(DATE(last_used_ts)), MIN(DATE(last_used_ts)), DAY),
      GREATEST(COUNT(*) - 1, 1)
    ) AS avg_days_between_uses
  FROM `of-scheduler-proj.core.v_caption_last_used_v3`
  WHERE last_used_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND last_used_ts IS NOT NULL
    AND caption_hash IS NOT NULL
    AND username_page IS NOT NULL
  GROUP BY 1, 2
),
fatigue_calc AS (
  SELECT
    caption_hash, 
    username_page, 
    times_used_30d, 
    most_recent_use, 
    first_use_30d, 
    usage_dates,
    avg_days_between_uses,
    
    -- Base fatigue from frequency
    CASE 
      WHEN times_used_30d >= 8 THEN 1.0   -- Extremely fatigued
      WHEN times_used_30d >= 5 THEN 0.8   -- Highly fatigued
      WHEN times_used_30d >= 3 THEN 0.5   -- Moderately fatigued
      WHEN times_used_30d >= 2 THEN 0.3   -- Lightly fatigued
      ELSE 0.1                            -- Fresh
    END AS base_fatigue_score,
    
    -- Recency multiplier (recent use = higher fatigue)
    1.2 - SAFE_DIVIDE(
      DATE_DIFF(CURRENT_DATE(), DATE(most_recent_use), DAY), 
      30.0
    ) AS recency_multiplier,
    
    -- Usage density (uses per active day)
    SAFE_DIVIDE(
      times_used_30d,
      GREATEST(ARRAY_LENGTH(usage_dates), 1)
    ) AS usage_density
    
  FROM usage_velocity
)
SELECT
  *,
  -- Final fatigue score (0.0 = fresh, 1.0+ = highly fatigued)
  GREATEST(
    base_fatigue_score * GREATEST(recency_multiplier, 0.5),
    0.0
  ) AS fatigue_score,
  
  -- Fatigue tier for easy filtering
  CASE
    WHEN base_fatigue_score * GREATEST(recency_multiplier, 0.5) >= 0.8 THEN 'BURNT_OUT'
    WHEN base_fatigue_score * GREATEST(recency_multiplier, 0.5) >= 0.5 THEN 'FATIGUED'
    WHEN base_fatigue_score * GREATEST(recency_multiplier, 0.5) >= 0.3 THEN 'MODERATE'
    ELSE 'FRESH'
  END AS fatigue_tier,
  
  -- Days since last use
  DATE_DIFF(CURRENT_DATE(), DATE(most_recent_use), DAY) AS days_since_last_use,
  
  CURRENT_TIMESTAMP() AS calculated_at
FROM fatigue_calc;



-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Test sentiment analysis
SELECT 
  'Sentiment Analysis Test' AS test_name,
  COUNT(*) AS total_captions,
  AVG(composite_energy) AS avg_energy,
  AVG(emoji_density_pct) AS avg_emoji_density,
  COUNTIF(engagement_prediction_score > 0.7) AS high_engagement_captions
FROM `of-scheduler-proj.core.v_caption_sentiment_v1`;

-- Test engagement patterns
SELECT 
  'Engagement Patterns Test' AS test_name,
  COUNT(*) AS pages_analyzed,
  AVG(schedule_coverage) AS avg_coverage,
  AVG(revenue_consistency) AS avg_consistency
FROM `of-scheduler-proj.core.v_page_engagement_patterns_v1`;

-- Test fatigue scoring
SELECT 
  'Fatigue Scoring Test' AS test_name,
  COUNT(*) AS caption_page_combinations,
  COUNT(DISTINCT caption_hash) AS unique_captions,
  COUNT(DISTINCT username_page) AS unique_pages,
  AVG(fatigue_score) AS avg_fatigue,
  COUNTIF(fatigue_tier = 'BURNT_OUT') AS burnt_out_count
FROM `of-scheduler-proj.core.caption_fatigue_scores_v1`;