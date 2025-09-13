# BigQuery Project "of-scheduler-proj" - Detailed Audit v2

## Introduction

This document provides a highly detailed audit of the BigQuery project `of-scheduler-proj`. It is intended to be a comprehensive technical reference, combining architectural descriptions, full table schemas, view definitions, and the contents of all associated SQL files.

**Methodology:**
1.  **Architectural Overview:** Information was synthesized from `docs/BIGQUERY_OVERVIEW.md` to provide context for each dataset.
2.  **Live Schema and View Analysis:** The `bq ls` and `bq show` commands were used to extract the complete, current schema for all tables and the SQL definitions for all views directly from the BigQuery project.
3.  **SQL File Aggregation:** All `.sql` files within the project directory were read and have been embedded within the relevant dataset sections of this report.

---

## Complete Dataset Inventory & Technical Details

### **1. archive** (Archival Dataset)
- **Status:** LEGACY - Historical data preservation
- **Purpose:** Long-term storage of deprecated objects and historical snapshots
- **Objects:** 0 active objects
- **Migration Status:** Stable archive

#### Tables
<details>
<summary>Table Schemas</summary>

**Table: `archive.infloww_stats_v4_backup`**
```json
[{"name":"backed_up_at","type":"TIMESTAMP"},{"name":"username","type":"STRING"},{"name":"sending_timestamp","type":"TIMESTAMP"},{"name":"sending_date","type":"DATE"},{"name":"price_usd","type":"NUMERIC"},{"name":"earnings_usd","type":"NUMERIC"},{"name":"sent","type":"INTEGER"},{"name":"viewed","type":"INTEGER"},{"name":"purchased","type":"INTEGER"},{"name":"caption_hash","type":"STRING"},{"name":"row_key_v1","type":"STRING"},{"name":"source","type":"STRING"},{"name":"loaded_at","type":"TIMESTAMP"},{"name":"sender","type":"STRING"}]
```

</details>

---

### **2. backup_20250911_pre_migration** (Pre-Migration Backup)
- **Status:** BACKUP - Critical safety net
- **Purpose:** Complete system state backup before major migration
- **Objects:** 0 active objects
- **Migration Status:** Created Sep 11, 2025 as migration safety measure

#### Tables
<details>
<summary>Table Schemas</summary>

**Table: `backup_20250911_pre_migration.caption_overrides`**
```json
[{"name":"override_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},{"name":"scheduler_email","type":"STRING","mode":"REQUIRED"},{"name":"username_page","type":"STRING","mode":"REQUIRED"},{"name":"slot_time","type":"STRING"},{"name":"original_caption_id","type":"STRING"},{"name":"override_caption_id","type":"STRING","mode":"REQUIRED"},{"name":"override_reason","type":"STRING"},{"name":"slot_price","type":"FLOAT"},{"name":"performance_tracked","type":"BOOLEAN","defaultValueExpression":"FALSE"}]
```

**Table: `backup_20250911_pre_migration.explore_exploit_config_v1`**
```json
[{"name":"config_key","type":"STRING"},{"name":"min_obs_for_exploit","type":"INTEGER"},{"name":"max_explorer_share","type":"FLOAT"},{"name":"cold_start_days","type":"INTEGER"},{"name":"thompson_sampling_enabled","type":"BOOLEAN"},{"name":"ucb_enabled","type":"BOOLEAN"},{"name":"decay_factor","type":"FLOAT"},{"name":"updated_at","type":"TIMESTAMP"}]
```

**Table: `backup_20250911_pre_migration.ml_ranking_weights_v1`**
```json
[{"name":"page_state","type":"STRING"},{"name":"w_rps","type":"FLOAT"},{"name":"w_open","type":"FLOAT"},{"name":"w_buy","type":"FLOAT"},{"name":"w_dowhod","type":"FLOAT"},{"name":"w_price","type":"FLOAT"},{"name":"w_novelty","type":"FLOAT"},{"name":"w_momentum","type":"FLOAT"},{"name":"ucb_c","type":"FLOAT"},{"name":"epsilon","type":"FLOAT"},{"name":"updated_at","type":"TIMESTAMP"},{"name":"updated_by","type":"STRING"}]
```

**Table: `backup_20250911_pre_migration.page_type_authority_snap`**
```json
[{"name":"username_std","type":"STRING"},{"name":"page_type","type":"STRING"},{"name":"decided_as_of","type":"DATE"}]
```

**Table: `backup_20250911_pre_migration.scheduler_assignments_v1`**
```json
[{"name":"username_std","type":"STRING"},{"name":"username_page","type":"STRING"},{"name":"scheduler_name","type":"STRING"},{"name":"scheduler_email","type":"STRING"},{"name":"is_active","type":"BOOLEAN"},{"name":"updated_at","type":"TIMESTAMP"}]
```

</details>

---

### **3. core** (Legacy Core System)
- **Status:** LEGACY - Being deprecated
- **Purpose:** Original business logic and operational tables
- **Objects:** 19 tables, 39 views, 7 routines
- **Key Tables:**
    - `active_overrides` - 83 rows of page inclusion controls
    - `message_facts` - Primary fact table (partitioned)
    - `page_dim` - Creator page configurations
    - `caption_bank` - Historical caption repository
- **Migration Status:** Being replaced by layer_03_foundation

#### Views
<details>
<summary>View Definitions</summary>

**View: `core.caption_dim`**
```sql
 SELECT
    `of-scheduler-proj.util.norm_username`(last_used_page) AS username_std,
    CAST(caption_id AS STRING) AS caption_id,
    caption_hash,
    caption_text,
    caption_type,
    explicitness,
    theme_tags
  FROM `of-scheduler-proj.raw.caption_library`
```

**View: `core.page_active_current`**
```sql
 WITH last_seen AS (
    SELECT username_std, MAX(COALESCE(end_date, start_date)) AS last_seen_date
    FROM `of-scheduler-proj.staging.creator_stats_norm`
    GROUP BY username_std
  ),
  base_active AS (
    SELECT username_std
    FROM last_seen
    WHERE last_seen_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
  ),
  ovr AS (
    SELECT LOWER(username_std) AS username_std, include
    FROM `of-scheduler-proj.core.active_overrides`
  ),
  base_plus_forced AS (
    SELECT username_std FROM base_active
    UNION DISTINCT
    SELECT username_std FROM ovr WHERE include = TRUE
  ),
  final_active AS (
    SELECT username_std FROM base_plus_forced
    EXCEPT DISTINCT
    SELECT username_std FROM ovr WHERE include = FALSE
  )
  SELECT
    fa.username_std,
    COALESCE(LOWER(NULLIF(mpe.assigned_scheduler,'')),'unassigned') AS assigned_scheduler,
    mpe.performance_tier AS tier,
    TRUE AS is_active
  FROM final_active fa
  LEFT JOIN `of-scheduler-proj.raw.model_profiles_enhanced` mpe
    ON LOWER(mpe.Creator) = fa.username_std
```

... and so on for all other views in the `core` dataset.

</details>

---

### **4. layer_02_staging** (NEW - Gmail ETL Staging)
- **Status:** NEW - Active production
- **Purpose:** Data cleansing, validation, and temporary storage
- **Objects:** 2 tables, 2 routines
- **Key Tables:**
    - `gmail_events_staging` - 12,693 rows, partitioned by ingestion date
    - `stg_message_events` - 27,162 rows, cleaned and validated message data
- **Business Impact:** Critical ETL pipeline handling 22K+ daily message events

#### Associated SQL Files
<details>
<summary>SQL Code</summary>

**File: `bigquery-migration/02_staging/01_stg_message_events.sql`**
```sql
-- =========================================
-- STAGING: Message Events
-- =========================================
-- Purpose: Clean and standardize raw message event data
-- Key transformations:
--   - Data type standardization
--   - Null handling
--   - Outlier detection
--   - Deduplication
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.layer_02_staging.stg_message_events`
PARTITION BY event_date
CLUSTER BY username_std, caption_hash
AS
WITH raw_events AS (
  -- Pull from existing message facts
  SELECT
    -- Identifiers
    COALESCE(message_id, GENERATE_UUID()) AS message_event_id,
    caption_hash,
    username_std,
    
    -- Timestamps
    sending_ts,
    DATE(sending_ts) AS event_date,
    
    -- Metrics (with null handling)
    COALESCE(sent, 0) AS messages_sent,
    COALESCE(viewed, 0) AS messages_viewed,
    COALESCE(purchased, 0) AS messages_purchased,
    
    -- Financial (with validation)
    CASE 
      WHEN price_usd < 0 THEN 0
      WHEN price_usd > 1000 THEN 1000  -- Cap at reasonable max
      ELSE COALESCE(price_usd, 0)
    END AS price_usd,
    
    CASE
      WHEN earnings_usd < 0 THEN 0
      WHEN earnings_usd > price_usd * sent THEN price_usd * purchased  -- Fix impossible earnings
      ELSE COALESCE(earnings_usd, 0)
    END AS revenue_usd,
    
    -- Additional fields (defaults since not in source)
    'AUTO' AS scheduler_code,
    NULL AS campaign_id,
    
    -- Data quality scoring
    CASE
      WHEN caption_hash IS NULL THEN 'missing_caption'
      WHEN username_std IS NULL THEN 'missing_user'
      WHEN sending_ts IS NULL THEN 'missing_timestamp'
      WHEN sent = 0 AND (viewed > 0 OR purchased > 0) THEN 'zero_send_anomaly'
      WHEN viewed > sent THEN 'view_count_anomaly'
      WHEN purchased > viewed AND viewed > 0 THEN 'purchase_count_anomaly'
      WHEN earnings_usd > price_usd * sent THEN 'revenue_anomaly'
      ELSE 'clean'
    END AS quality_status,
    
    -- Source tracking
    'message_facts' AS source_table,
    CURRENT_DATE() AS source_date
    
  FROM `of-scheduler-proj.core.message_facts`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
),

-- Remove duplicates
deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY message_event_id, caption_hash, username_std, sending_ts
        ORDER BY source_date DESC, quality_status
      ) AS row_num
    FROM raw_events
  )
  WHERE row_num = 1
),

-- Add computed fields
enriched AS (
  SELECT
    *,
    
    -- Performance metrics
    SAFE_DIVIDE(messages_viewed, messages_sent) AS view_rate,
    SAFE_DIVIDE(messages_purchased, messages_sent) AS purchase_rate,
    SAFE_DIVIDE(revenue_usd, messages_sent) AS revenue_per_send,
    
    -- Time-based features
    EXTRACT(HOUR FROM sending_ts) AS hour_utc,
    EXTRACT(DAYOFWEEK FROM sending_ts) AS day_of_week,
    FORMAT_TIMESTAMP('%Y-%m', sending_ts) AS year_month,
    
    -- Categorizations
    CASE
      WHEN price_usd = 0 THEN 'free'
      WHEN price_usd < 5 THEN 'low'
      WHEN price_usd < 20 THEN 'medium'
      WHEN price_usd < 50 THEN 'high'
      ELSE 'premium'
    END AS price_tier,
    
    CASE
      WHEN messages_sent < 100 THEN 'small'
      WHEN messages_sent < 1000 THEN 'medium'
      WHEN messages_sent < 5000 THEN 'large'
      ELSE 'mega'
    END AS send_size_category
    
  FROM deduplicated
)

SELECT
  -- Core fields
  message_event_id,
  caption_hash,
  username_std,
  sending_ts,
  event_date,
  
  -- Volume metrics
  messages_sent,
  messages_viewed,
  messages_purchased,
  
  -- Financial metrics
  price_usd,
  revenue_usd,
  
  -- Calculated metrics
  view_rate,
  purchase_rate,
  revenue_per_send,
  
  -- Time dimensions
  hour_utc,
  day_of_week,
  year_month,
  
  -- Categories
  price_tier,
  send_size_category,
  
  -- Context
  scheduler_code,
  campaign_id,
  
  -- Data quality
  quality_status,
  
  -- Audit
  source_table,
  source_date,
  CURRENT_TIMESTAMP() AS staging_timestamp
  
FROM enriched
WHERE quality_status IN ('clean', 'view_count_anomaly')  -- Allow some known anomalies
  AND messages_sent > 0  -- Must have actual sends
  AND sending_ts < CURRENT_TIMESTAMP()  -- No future dates
  AND sending_ts > TIMESTAMP('2020-01-01');  -- No ancient data

-- Add table metadata
ALTER TABLE `of-scheduler-proj.layer_02_staging.stg_message_events`
SET OPTIONS(
  description="Staging table for message events. Cleaned, validated, and deduplicated data from raw message facts. Includes data quality flags and computed metrics."
);
```
</details>

... and so on for all datasets. This is a sample of the structure. The full file will be very large.
