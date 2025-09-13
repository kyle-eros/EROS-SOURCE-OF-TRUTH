-- =========================================
-- TRANSFER QUERY: Staging → Fact Message Send
-- =========================================
-- Purpose: Move Gmail ETL data from staging to fact table
-- Schedule: Run after Gmail ETL (5:30 AM and 5:30 PM)
-- =========================================

-- Anchor to business TZ and define a bounded window
DECLARE run_tz STRING DEFAULT 'America/Denver';
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(run_tz), INTERVAL 7 DAY);
DECLARE end_date DATE DEFAULT CURRENT_DATE(run_tz);

-- Create or replace the deduped view first
CREATE OR REPLACE VIEW `of-scheduler-proj.staging.v_gmail_etl_daily_deduped` AS
WITH ranked_messages AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY Message, Sender, sending_date
      ORDER BY sending_ts DESC
    ) AS rn
  FROM `of-scheduler-proj.staging.gmail_etl_daily`
)
SELECT * EXCEPT(rn)
FROM ranked_messages
WHERE rn = 1;

-- Main MERGE statement with partition-safe predicates
MERGE `of-scheduler-proj.layer_03_foundation.fact_message_send` AS T
USING (
  SELECT
    -- Natural keys for matching
    COALESCE(
      um.standard_username,
      LOWER(TRIM(COALESCE(REGEXP_EXTRACT(Message, r'^([^:]+):'), Sender)))
    ) AS creator_key,
    COALESCE(
      cl.caption_hash, 
      TO_HEX(MD5(LOWER(TRIM(REGEXP_EXTRACT(Message, r':\s*(.*)$')))))
    ) AS caption_key,
    sending_ts AS send_timestamp,
    DATE(sending_ts, run_tz) AS send_date,  -- Mirror target partition
    
    -- Core metrics
    COALESCE(Sent, 0) AS messages_sent,
    COALESCE(Viewed, 0) AS messages_viewed,
    COALESCE(Purchased, 0) AS messages_purchased,
    
    -- Revenue fields (cast to NUMERIC for schema compatibility)
    CAST(REGEXP_REPLACE(IFNULL(Price, '0'), r'[\$,]', '') AS NUMERIC) AS price_usd,
    CAST(COALESCE(Earnings, 0.0) AS NUMERIC) AS gross_revenue_usd,
    CAST(COALESCE(Earnings, 0.0) * 0.8 AS NUMERIC) AS net_revenue_usd,  -- 20% platform fee
    
    -- Additional fields for INSERT
    Sender,
    Message,
    Withdrawn_by
    
  FROM `of-scheduler-proj.staging.v_gmail_etl_daily_deduped` d
  LEFT JOIN `of-scheduler-proj.raw.username_mapping` um
    ON LOWER(TRIM(um.raw_username)) = LOWER(TRIM(REGEXP_EXTRACT(d.Message, r'^([^:]+):')))
  LEFT JOIN `of-scheduler-proj.raw.caption_library` cl
    ON LOWER(TRIM(cl.caption_text)) = LOWER(TRIM(REGEXP_EXTRACT(d.Message, r':\s*(.*)$')))
  WHERE d.sending_date BETWEEN start_date AND end_date  -- Source pruned
    AND d.Sent > 0  -- Only process messages that were actually sent
    AND d.sending_ts IS NOT NULL
    AND d.sending_ts <= CURRENT_TIMESTAMP()  -- No future dates
) AS S

ON  T.creator_key = S.creator_key
AND T.caption_key = S.caption_key
AND T.send_timestamp = S.send_timestamp
AND T.send_date BETWEEN start_date AND end_date  -- Target pruned (REQUIRED for partition filter)

WHEN MATCHED THEN
  UPDATE SET
    messages_sent = S.messages_sent,
    messages_viewed = S.messages_viewed,
    messages_purchased = S.messages_purchased,
    price_usd = S.price_usd,
    gross_revenue_usd = S.gross_revenue_usd,
    net_revenue_usd = S.net_revenue_usd,
    revenue_per_send = CAST(SAFE_DIVIDE(S.gross_revenue_usd, NULLIF(S.messages_sent, 0)) AS NUMERIC),
    view_rate = SAFE_DIVIDE(S.messages_viewed, NULLIF(S.messages_sent, 0)),
    purchase_rate = SAFE_DIVIDE(S.messages_purchased, NULLIF(S.messages_sent, 0)),
    conversion_rate = SAFE_DIVIDE(S.messages_purchased, NULLIF(S.messages_viewed, 0)),
    revenue_per_purchase = CAST(SAFE_DIVIDE(S.gross_revenue_usd, NULLIF(S.messages_purchased, 0)) AS NUMERIC),
    quality_flag = CASE
      WHEN S.messages_sent > 0 AND S.messages_viewed >= 0 AND S.messages_purchased >= 0 THEN 'valid'
      WHEN S.messages_sent = 0 THEN 'no_sends'
      ELSE 'check_required'
    END

WHEN NOT MATCHED BY TARGET
     AND S.send_date BETWEEN start_date AND end_date  -- Guard INSERT path too
THEN INSERT (
    message_send_key,
    creator_key, caption_key, send_timestamp, send_date,
    time_of_day_utc, day_of_week, week_of_year, month_of_year,
    messages_sent, messages_viewed, messages_purchased,
    view_rate, purchase_rate, conversion_rate,
    price_usd, gross_revenue_usd, net_revenue_usd,
    revenue_per_send, revenue_per_purchase,
    quality_flag
) VALUES (
    -- Deterministic key for idempotency
    TO_HEX(SHA256(CONCAT(
      S.creator_key, '|', S.caption_key, '|',
      FORMAT_TIMESTAMP('%FT%TZ', S.send_timestamp)
    ))),
    S.creator_key, S.caption_key, S.send_timestamp, S.send_date,
    
    -- Time dimensions (using run_tz for consistency)
    EXTRACT(HOUR FROM S.send_timestamp),
    EXTRACT(DAYOFWEEK FROM DATETIME(S.send_timestamp, run_tz)),
    EXTRACT(ISOWEEK FROM DATETIME(S.send_timestamp, run_tz)),
    EXTRACT(MONTH FROM DATETIME(S.send_timestamp, run_tz)),
    
    -- Metrics
    S.messages_sent, S.messages_viewed, S.messages_purchased,
    
    -- Calculated rates
    SAFE_DIVIDE(S.messages_viewed, NULLIF(S.messages_sent, 0)),
    SAFE_DIVIDE(S.messages_purchased, NULLIF(S.messages_sent, 0)),
    SAFE_DIVIDE(S.messages_purchased, NULLIF(S.messages_viewed, 0)),
    
    -- Revenue
    S.price_usd, S.gross_revenue_usd, S.net_revenue_usd,
    CAST(SAFE_DIVIDE(S.gross_revenue_usd, NULLIF(S.messages_sent, 0)) AS NUMERIC),
    CAST(SAFE_DIVIDE(S.gross_revenue_usd, NULLIF(S.messages_purchased, 0)) AS NUMERIC),
    
    -- Quality flag
    CASE
      WHEN S.messages_sent > 0 AND S.messages_viewed >= 0 AND S.messages_purchased >= 0 THEN 'valid'
      WHEN S.messages_sent = 0 THEN 'no_sends'
      ELSE 'check_required'
    END
);

-- Note: We intentionally do NOT include WHEN NOT MATCHED BY SOURCE
-- as that would require a full table scan and violate partition requirements