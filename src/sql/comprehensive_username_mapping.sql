-- =====================================================
-- COMPREHENSIVE USERNAME MAPPING
-- Map historical CSV usernames to current active creators
-- =====================================================

CREATE OR REPLACE VIEW `of-scheduler-proj.staging.v_username_mapping` AS
SELECT 
  historical_name,
  current_name,
  'exact_match' as match_type
FROM UNNEST([
  -- Direct matches
  STRUCT('itskassielee' as historical_name, 'itskassielee' as current_name),
  STRUCT('tessatan' as historical_name, 'tessatan' as current_name),
  STRUCT('oliviahansley' as historical_name, 'olivia hansley' as current_name),
  STRUCT('olivia.hansleyyy' as historical_name, 'olivia hansley' as current_name),
  STRUCT('misslexa' as historical_name, 'miss lexa' as current_name),
  STRUCT('corvettemykala' as historical_name, 'corvettemykala' as current_name),
  STRUCT('chloewildd' as historical_name, 'chloe wildd' as current_name),
  STRUCT('sweet.grace' as historical_name, 'grace bennett' as current_name),
  STRUCT('sweetgracee' as historical_name, 'grace bennett' as current_name),
  STRUCT('miafosterxx' as historical_name, 'mia foster' as current_name),
  STRUCT('miaharperrrrrr' as historical_name, 'mia harper' as current_name),
  STRUCT('alex.loveee' as historical_name, 'alex love' as current_name),
  STRUCT('bellajadeee' as historical_name, 'bella jade' as current_name),
  STRUCT('delsbigworld' as historical_name, 'del' as current_name),
  STRUCT('adriannarodriguezxo' as historical_name, 'adrianna rodriguez' as current_name),
  STRUCT('aprilmayxxx' as historical_name, 'april may' as current_name),
  STRUCT('calilov3r' as historical_name, 'cali love' as current_name),
  STRUCT('carmenxrosse' as historical_name, 'carmen rose' as current_name),
  STRUCT('scarlettgraceee' as historical_name, 'scarlett grace' as current_name),
  STRUCT('scarlettmartinn' as historical_name, 'scarlette rose' as current_name),
  STRUCT('sophiaroseexx' as historical_name, 'sophia grace' as current_name),
  STRUCT('stellaraeeee' as historical_name, 'stella corbet' as current_name),
  STRUCT('thestormii' as historical_name, 'stormii' as current_name),
  STRUCT('tittytalia' as historical_name, 'titty talia' as current_name),
  STRUCT('tori.rae' as historical_name, 'tori rae' as current_name),
  STRUCT('jadewilkinsonn' as historical_name, 'jade wilkinson' as current_name),
  STRUCT('kayclaireee' as historical_name, 'kay claire' as current_name),
  STRUCT('kellymedonly' as historical_name, 'kelly love' as current_name),
  STRUCT('madisynmaee' as historical_name, 'madison summers' as current_name),
  STRUCT('neenahbrownn' as historical_name, 'neenah' as current_name),
  STRUCT('poutyselena' as historical_name, 'selena' as current_name),
  STRUCT('realselenarae' as historical_name, 'selena' as current_name),
  STRUCT('winterskyeee' as historical_name, 'skye' as current_name),
  STRUCT('itslolariv' as historical_name, 'lola rivers' as current_name),
  STRUCT('itsjustclaire' as historical_name, 'caroline mae' as current_name),
  STRUCT('clairethompson' as historical_name, 'caroline mae' as current_name)
]) as mapping;

-- Create enhanced filtered view with ALL historical data
CREATE OR REPLACE VIEW `of-scheduler-proj.staging.v_all_historical_enhanced` AS
WITH all_historical AS (
  SELECT 
    h.*,
    -- Map usernames using comprehensive mapping
    COALESCE(m.current_name, 
      CASE 
        WHEN LOWER(h.username_raw) = 'misslexa' THEN 'miss lexa'
        WHEN LOWER(h.username_raw) = 'itskassielee' THEN 'itskassielee'
        WHEN LOWER(h.username_raw) = 'oliviahansley' THEN 'olivia hansley'
        WHEN LOWER(h.username_raw) = 'tessatan' THEN 'tessatan'
        WHEN LOWER(h.username_raw) = 'michellegxoxo' THEN 'michelle gxoxo'
        ELSE LOWER(REPLACE(h.username_raw, '_', ' '))
      END
    ) AS username_normalized,
    
    -- Mark if this is a current active creator
    CASE WHEN m.current_name IS NOT NULL THEN TRUE ELSE FALSE END as is_current_active,
    
    -- Generate message_id and caption_hash
    GENERATE_UUID() AS message_id,
    TO_HEX(SHA256(h.message_text)) AS caption_hash,
    
    -- Parse sending time
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', h.sending_time) AS sending_ts,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', h.sending_time)) AS sending_date,
    
    -- Parse price
    CASE 
      WHEN h.price_usd_raw = '0' THEN 0
      ELSE CAST(h.price_usd_raw AS NUMERIC)
    END AS price_usd,
    
    -- Generate row key
    CONCAT(
      'hist_',
      GENERATE_UUID(),
      '_',
      FORMAT_TIMESTAMP('%Y%m%d', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', h.sending_time))
    ) AS row_key_v1
    
  FROM `of-scheduler-proj.staging.historical_message_staging` h
  LEFT JOIN `of-scheduler-proj.staging.v_username_mapping` m
    ON LOWER(h.username_raw) = LOWER(m.historical_name)
  WHERE h.message_text IS NOT NULL 
    AND h.sending_time IS NOT NULL
    AND h.earnings_usd >= 0
    AND h.username_raw IS NOT NULL
)
SELECT * FROM all_historical;

-- Analysis: Show current active creators found
SELECT 
  'CURRENT ACTIVE CREATORS FOUND' as analysis,
  username_normalized,
  COUNT(*) as historical_messages,
  ROUND(SUM(earnings_usd), 2) as total_revenue,
  MIN(sending_ts) as earliest_date,
  MAX(sending_ts) as latest_date
FROM `of-scheduler-proj.staging.v_all_historical_enhanced`
WHERE is_current_active = TRUE
GROUP BY username_normalized
ORDER BY historical_messages DESC;

-- Analysis: All historical data summary
SELECT 
  'ALL HISTORICAL DATA SUMMARY' as summary,
  COUNT(*) as total_records,
  COUNT(DISTINCT username_normalized) as unique_creators,
  SUM(CASE WHEN is_current_active THEN 1 ELSE 0 END) as current_active_records,
  COUNT(DISTINCT CASE WHEN is_current_active THEN username_normalized END) as current_active_creators,
  ROUND(SUM(earnings_usd), 2) as total_revenue,
  MIN(sending_ts) as earliest_date,
  MAX(sending_ts) as latest_date
FROM `of-scheduler-proj.staging.v_all_historical_enhanced`;