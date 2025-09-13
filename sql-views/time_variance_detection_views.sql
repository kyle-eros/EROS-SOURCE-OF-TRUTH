-- =====================================================
-- TIME VARIANCE DETECTION & ANTI-PATTERN ALERTS 
-- =====================================================
-- This file contains views for detecting robotic timing patterns
-- and ensuring schedulers vary posting times to maintain engagement.

-- Core Time Variance Detection
-- Analyzes recent sending patterns to detect robotic behavior
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_time_variance_detection` AS
WITH
-- Recent sends with timing analysis (last 14 days)
recent_sends AS (
  SELECT
    username_page,
    username_std,
    sending_ts,
    sending_date,
    EXTRACT(HOUR FROM sending_ts) AS hod,
    EXTRACT(MINUTE FROM sending_ts) AS minute,
    EXTRACT(DAYOFWEEK FROM sending_ts) AS dow,
    price_usd,
    earnings_usd,
    
    -- Calculate time differences between consecutive sends
    LAG(EXTRACT(HOUR FROM sending_ts), 1) OVER (
      PARTITION BY username_page 
      ORDER BY sending_ts
    ) AS prev_hod,
    LAG(EXTRACT(HOUR FROM sending_ts), 2) OVER (
      PARTITION BY username_page 
      ORDER BY sending_ts
    ) AS prev_hod2,
    LAG(EXTRACT(HOUR FROM sending_ts), 3) OVER (
      PARTITION BY username_page 
      ORDER BY sending_ts
    ) AS prev_hod3,
    
    LAG(sending_date, 1) OVER (
      PARTITION BY username_page 
      ORDER BY sending_ts
    ) AS prev_date,
    
    -- Same day sends (to detect burst patterns)
    COUNT(*) OVER (
      PARTITION BY username_page, sending_date
    ) AS sends_same_day,
    
    ROW_NUMBER() OVER (
      PARTITION BY username_page, sending_date 
      ORDER BY sending_ts
    ) AS daily_send_rank
    
  FROM `of-scheduler-proj.core.v_message_facts_by_page`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    AND sending_ts <= CURRENT_TIMESTAMP()
  ORDER BY username_page, sending_ts
),

-- Pattern detection calculations
pattern_analysis AS (
  SELECT
    username_page,
    username_std,
    sending_date,
    hod,
    minute,
    dow,
    prev_hod,
    prev_hod2, 
    prev_hod3,
    prev_date,
    sends_same_day,
    daily_send_rank,
    
    -- Time difference patterns
    ABS(hod - COALESCE(prev_hod, hod)) AS hod_diff_1,
    ABS(COALESCE(prev_hod, hod) - COALESCE(prev_hod2, prev_hod, hod)) AS hod_diff_2,
    ABS(COALESCE(prev_hod2, prev_hod, hod) - COALESCE(prev_hod3, prev_hod2, prev_hod, hod)) AS hod_diff_3,
    
    -- Exact time repetition (within 5 minutes)
    CASE WHEN ABS(hod - COALESCE(prev_hod, hod)) <= 0 
              AND ABS(minute - LAG(minute, 1) OVER (PARTITION BY username_page ORDER BY sending_ts)) <= 5
         THEN 1 ELSE 0 END AS exact_time_repeat,
    
    -- Day gap analysis  
    DATE_DIFF(sending_date, COALESCE(prev_date, sending_date), DAY) AS day_gap,
    
    -- Same hour different days detection
    CASE WHEN hod = prev_hod 
              AND DATE_DIFF(sending_date, prev_date, DAY) = 1
         THEN 1 ELSE 0 END AS consecutive_day_same_hour
    
  FROM recent_sends
),

-- Aggregate pattern metrics per page
page_pattern_summary AS (
  SELECT
    username_page,
    username_std,
    COUNT(*) AS total_sends_14d,
    COUNT(DISTINCT sending_date) AS active_days,
    COUNT(DISTINCT hod) AS unique_hours_used,
    
    -- Robotic timing indicators
    SUM(CASE WHEN hod_diff_1 <= 1 THEN 1 ELSE 0 END) AS very_close_consecutive_times,
    SUM(CASE WHEN hod_diff_1 <= 1 AND hod_diff_2 <= 1 THEN 1 ELSE 0 END) AS robotic_sequence_3,
    SUM(exact_time_repeat) AS exact_repeats,
    SUM(consecutive_day_same_hour) AS same_hour_consecutive_days,
    
    -- Time diversity metrics
    STDDEV(hod) AS hour_diversity_stddev,
    COUNT(DISTINCT CONCAT(hod, '_', minute DIV 15)) AS unique_time_windows,  -- 15-min windows
    
    -- Most common hour analysis
    MODE(hod) AS most_common_hour,
    COUNTIF(hod = MODE(hod)) AS most_common_hour_count,
    SAFE_DIVIDE(COUNTIF(hod = MODE(hod)), COUNT(*)) AS most_common_hour_ratio,
    
    -- Burst sending patterns
    AVG(sends_same_day) AS avg_sends_per_day,
    MAX(sends_same_day) AS max_sends_per_day,
    COUNTIF(sends_same_day > 3) AS burst_days,
    
    -- Day of week patterns
    COUNT(DISTINCT dow) AS unique_dows_used,
    STDDEV(dow) AS dow_diversity,
    
    -- Time window analysis (morning/afternoon/evening)
    COUNTIF(hod BETWEEN 6 AND 11) AS morning_sends,
    COUNTIF(hod BETWEEN 12 AND 17) AS afternoon_sends, 
    COUNTIF(hod BETWEEN 18 AND 23) AS evening_sends,
    COUNTIF(hod BETWEEN 0 AND 5) AS late_night_sends
    
  FROM pattern_analysis  
  GROUP BY username_page, username_std
),

-- Risk scoring and alert classification
risk_scoring AS (
  SELECT
    pps.*,
    
    -- Calculate robotic behavior risk score (0-100)
    LEAST(100, GREATEST(0, 
      -- Base penalty for low diversity
      (CASE WHEN unique_hours_used <= 2 THEN 25 ELSE 0 END) +
      
      -- Penalty for exact repeats
      (exact_repeats * 5) +
      
      -- Penalty for consecutive similar times
      (LEAST(very_close_consecutive_times * 3, 20)) +
      
      -- Penalty for robotic sequences
      (robotic_sequence_3 * 8) +
      
      -- Penalty for same hour consecutive days
      (same_hour_consecutive_days * 4) +
      
      -- Penalty for over-reliance on single hour
      (CASE WHEN most_common_hour_ratio > 0.6 THEN 15 
            WHEN most_common_hour_ratio > 0.4 THEN 8 
            ELSE 0 END) +
            
      -- Penalty for poor time diversity
      (CASE WHEN hour_diversity_stddev < 2.0 THEN 10 ELSE 0 END) +
      
      -- Penalty for limited time windows
      (CASE WHEN unique_time_windows <= 3 THEN 8 ELSE 0 END)
      
    )) AS robotic_risk_score,
    
    -- Engagement risk (monotonous scheduling hurts engagement)
    LEAST(100, GREATEST(0,
      -- Single time window dominance
      (CASE WHEN GREATEST(morning_sends, afternoon_sends, evening_sends) > 0.8 * total_sends_14d THEN 20 ELSE 0 END) +
      
      -- DOW monotony
      (CASE WHEN unique_dows_used <= 2 AND active_days > 5 THEN 15 ELSE 0 END) +
      
      -- Burst vs spacing balance
      (CASE WHEN burst_days > active_days * 0.5 THEN 10 ELSE 0 END) +
      
      -- Minimal variance 
      (CASE WHEN hour_diversity_stddev < 1.5 THEN 12 ELSE 0 END)
      
    )) AS engagement_risk_score,
    
    CURRENT_TIMESTAMP() AS calculated_at
    
  FROM page_pattern_summary pps
)

SELECT
  rs.username_page,
  rs.username_std,
  rs.total_sends_14d,
  rs.active_days,
  rs.unique_hours_used,
  rs.unique_dows_used,
  
  -- Risk scores
  rs.robotic_risk_score,
  rs.engagement_risk_score,
  GREATEST(rs.robotic_risk_score, rs.engagement_risk_score) AS overall_risk_score,
  
  -- Risk classification
  CASE
    WHEN GREATEST(rs.robotic_risk_score, rs.engagement_risk_score) >= 70 THEN 'CRITICAL'
    WHEN GREATEST(rs.robotic_risk_score, rs.engagement_risk_score) >= 50 THEN 'HIGH'
    WHEN GREATEST(rs.robotic_risk_score, rs.engagement_risk_score) >= 30 THEN 'MEDIUM'
    WHEN GREATEST(rs.robotic_risk_score, rs.engagement_risk_score) >= 15 THEN 'LOW' 
    ELSE 'MINIMAL'
  END AS risk_level,
  
  -- Pattern details
  rs.most_common_hour,
  rs.most_common_hour_count,
  rs.most_common_hour_ratio,
  rs.hour_diversity_stddev,
  rs.unique_time_windows,
  
  -- Robotic indicators
  rs.very_close_consecutive_times,
  rs.robotic_sequence_3,
  rs.exact_repeats,
  rs.same_hour_consecutive_days,
  
  -- Time distribution
  rs.morning_sends,
  rs.afternoon_sends,
  rs.evening_sends,
  rs.late_night_sends,
  
  -- Burst patterns
  rs.avg_sends_per_day,
  rs.max_sends_per_day,
  rs.burst_days,
  
  rs.calculated_at

FROM risk_scoring rs
ORDER BY rs.overall_risk_score DESC, rs.username_page;


-- Time Variance Alerts & Recommendations
-- Generates specific actionable alerts for schedulers
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_time_variance_alerts` AS
WITH
-- Get current risk data
risk_data AS (
  SELECT * FROM `of-scheduler-proj.core.v_time_variance_detection`
  WHERE total_sends_14d >= 3  -- Only alert for pages with meaningful data
),

-- Generate specific alerts
alert_generation AS (
  SELECT
    rd.username_page,
    rd.username_std,
    rd.risk_level,
    rd.robotic_risk_score,
    rd.engagement_risk_score,
    rd.overall_risk_score,
    
    -- Generate alert array
    ARRAY(
      SELECT alert FROM UNNEST([
        -- Robotic timing alerts
        CASE WHEN rd.robotic_sequence_3 >= 3 
             THEN STRUCT('ROBOTIC_TIMING' as alert_type, 'CRITICAL' as severity,
                        'Detected robotic timing: 3+ consecutive sends within 1-hour windows' as message,
                        'Vary sending times by at least 2-3 hours between messages' as recommendation)
        END,
        
        CASE WHEN rd.exact_repeats >= 2
             THEN STRUCT('EXACT_REPEATS' as alert_type, 'HIGH' as severity,  
                        CONCAT('Found ', CAST(rd.exact_repeats AS STRING), ' exact time repeats') as message,
                        'Use different minutes and vary hours by at least 30 minutes' as recommendation)
        END,
        
        CASE WHEN rd.most_common_hour_ratio > 0.5 
             THEN STRUCT('HOUR_DOMINANCE' as alert_type, 'MEDIUM' as severity,
                        CONCAT('Over-using hour ', CAST(rd.most_common_hour AS STRING), 
                               ' (', CAST(ROUND(rd.most_common_hour_ratio * 100, 1) AS STRING), '% of sends)') as message,
                        'Distribute sends across at least 4-5 different hours' as recommendation)
        END,
        
        CASE WHEN rd.same_hour_consecutive_days >= 3
             THEN STRUCT('CONSECUTIVE_SAME_HOUR' as alert_type, 'HIGH' as severity,
                        'Sending at same hour on consecutive days repeatedly' as message,  
                        'Vary daily send times - avoid same hour multiple days in row' as recommendation)
        END,
        
        -- Engagement risk alerts
        CASE WHEN rd.unique_hours_used <= 2 AND rd.total_sends_14d >= 8
             THEN STRUCT('LIMITED_TIME_DIVERSITY' as alert_type, 'HIGH' as severity,
                        CONCAT('Only using ', CAST(rd.unique_hours_used AS STRING), ' different hours') as message,
                        'Expand to at least 4-6 different sending hours' as recommendation)
        END,
        
        CASE WHEN rd.hour_diversity_stddev < 2.0 AND rd.total_sends_14d >= 6
             THEN STRUCT('LOW_TIME_VARIANCE' as alert_type, 'MEDIUM' as severity,
                        'Low time variance detected - sends clustered in narrow window' as message,
                        'Spread sends across broader time ranges (6+ hour span)' as recommendation) 
        END,
        
        CASE WHEN rd.unique_time_windows <= 3 AND rd.total_sends_14d >= 8  
             THEN STRUCT('NARROW_WINDOWS' as alert_type, 'MEDIUM' as severity,
                        'Sends limited to very few time windows' as message,
                        'Use more varied 15-minute windows throughout allowed hours' as recommendation)
        END,
        
        -- Time of day distribution alerts
        CASE WHEN GREATEST(rd.morning_sends, rd.afternoon_sends, rd.evening_sends) > 0.8 * rd.total_sends_14d
             THEN STRUCT('TIME_PERIOD_DOMINANCE' as alert_type, 'MEDIUM' as severity,
                        'Over 80% of sends in single time period (morning/afternoon/evening)' as message,
                        'Distribute sends across different parts of the day' as recommendation)
        END,
        
        -- DOW monotony
        CASE WHEN rd.unique_dows_used <= 2 AND rd.active_days >= 6
             THEN STRUCT('DOW_MONOTONY' as alert_type, 'LOW' as severity,
                        'Limited day-of-week diversity in sending pattern' as message,
                        'Vary sending across more days of the week' as recommendation)
        END,
        
        -- Burst pattern alerts
        CASE WHEN rd.burst_days > rd.active_days * 0.6 AND rd.max_sends_per_day > 4
             THEN STRUCT('EXCESSIVE_BURSTING' as alert_type, 'MEDIUM' as severity,
                        'Frequent burst sending days detected' as message,
                        'Space out sends more evenly - avoid clustering multiple sends per day' as recommendation)
        END
        
      ]) AS alert 
      WHERE alert IS NOT NULL
    ) AS alerts,
    
    rd.calculated_at
    
  FROM risk_data rd
),

-- Flatten alerts for easier querying
flattened_alerts AS (
  SELECT
    ag.username_page,
    ag.username_std, 
    ag.risk_level,
    ag.overall_risk_score,
    alert.alert_type,
    alert.severity,
    alert.message,
    alert.recommendation,
    ag.calculated_at
  FROM alert_generation ag
  CROSS JOIN UNNEST(ag.alerts) AS alert
)

SELECT
  fa.username_page,
  fa.username_std,
  fa.risk_level,
  fa.overall_risk_score,
  fa.alert_type,
  fa.severity,
  fa.message,
  fa.recommendation,
  
  -- Priority scoring for alert triage
  CASE fa.severity
    WHEN 'CRITICAL' THEN 100
    WHEN 'HIGH' THEN 75
    WHEN 'MEDIUM' THEN 50
    WHEN 'LOW' THEN 25
    ELSE 0
  END + fa.overall_risk_score * 0.3 AS alert_priority_score,
  
  fa.calculated_at

FROM flattened_alerts fa
ORDER BY alert_priority_score DESC, fa.username_page, fa.alert_type;


-- Time Variance Improvement Suggestions
-- Provides specific time slot suggestions to improve variance
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_time_variance_suggestions` AS
WITH
-- Current page constraints and preferences  
page_constraints AS (
  SELECT
    pd.username_std,
    COALESCE(pd.min_hod, 8) AS min_allowed_hour,
    COALESCE(pd.max_hod, 22) AS max_allowed_hour,
    pd.tz,
    
    -- Get current active hours from recent sends
    ARRAY_AGG(DISTINCT EXTRACT(HOUR FROM m.sending_ts) IGNORE NULLS 
              ORDER BY EXTRACT(HOUR FROM m.sending_ts)) AS current_active_hours
    
  FROM `of-scheduler-proj.core.page_dim` pd
  LEFT JOIN `of-scheduler-proj.core.v_message_facts_by_page` m 
    ON m.username_std = pd.username_std
    AND m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  WHERE COALESCE(pd.is_active, TRUE)
  GROUP BY pd.username_std, pd.min_hod, pd.max_hod, pd.tz
),

-- Historical performance by hour for each page
hour_performance AS (
  SELECT
    username_std,
    EXTRACT(HOUR FROM sending_ts) AS hod,
    COUNT(*) AS sends_count,
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS avg_conversion_rate,
    AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS avg_rps,
    SUM(earnings_usd) AS total_revenue
  FROM `of-scheduler-proj.core.v_message_facts_by_page`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND sent > 0
  GROUP BY username_std, EXTRACT(HOUR FROM sending_ts)
),

-- Risk assessment for each page
risk_info AS (
  SELECT
    username_page,
    username_std,
    risk_level,
    overall_risk_score,
    most_common_hour,
    most_common_hour_ratio,
    unique_hours_used,
    hour_diversity_stddev
  FROM `of-scheduler-proj.core.v_time_variance_detection`  
),

-- Generate improvement suggestions
suggestions AS (
  SELECT
    ri.username_page,
    ri.username_std,
    ri.risk_level,
    ri.overall_risk_score,
    pc.min_allowed_hour,
    pc.max_allowed_hour,
    pc.current_active_hours,
    ri.most_common_hour,
    ri.unique_hours_used,
    
    -- Suggest underutilized high-performing hours
    ARRAY(
      SELECT STRUCT(
        hp.hod as suggested_hour,
        ROUND(hp.avg_conversion_rate, 4) as performance_score,
        hp.sends_count as historical_sends,
        CASE 
          WHEN hp.hod NOT IN UNNEST(pc.current_active_hours) THEN 'New hour - high potential'
          WHEN hp.hod = ri.most_common_hour THEN 'Reduce usage - currently overused'
          ELSE 'Good alternative hour'
        END as suggestion_reason
      )
      FROM hour_performance hp
      WHERE hp.username_std = ri.username_std
        AND hp.hod BETWEEN pc.min_allowed_hour AND pc.max_allowed_hour
        AND hp.sends_count >= 2  -- Minimum data requirement
        AND (hp.hod NOT IN UNNEST(pc.current_active_hours) OR hp.avg_conversion_rate > 0.08)
      ORDER BY hp.avg_conversion_rate DESC, hp.avg_rps DESC
      LIMIT 6
    ) AS suggested_hours,
    
    -- Generate all valid hours in range for full alternatives
    ARRAY(
      SELECT hour 
      FROM UNNEST(GENERATE_ARRAY(pc.min_allowed_hour, pc.max_allowed_hour)) AS hour
      WHERE hour NOT IN UNNEST(COALESCE(pc.current_active_hours, []))
      ORDER BY ABS(hour - 14)  -- Prefer hours closer to 2 PM (general peak)
      LIMIT 8
    ) AS unused_hours_in_range,
    
    -- Specific improvement strategies
    CASE
      WHEN ri.risk_level IN ('CRITICAL', 'HIGH') AND ri.unique_hours_used <= 2 
        THEN 'EMERGENCY: Immediately expand to 4+ different hours. Stop using hour ' || CAST(ri.most_common_hour AS STRING) || ' for next 3 days.'
      WHEN ri.risk_level = 'HIGH' AND ri.most_common_hour_ratio > 0.5
        THEN 'HIGH PRIORITY: Reduce hour ' || CAST(ri.most_common_hour AS STRING) || ' usage to max 30% of sends. Add 2-3 new hours.'
      WHEN ri.risk_level = 'MEDIUM' 
        THEN 'MEDIUM PRIORITY: Gradually add 1-2 new sending hours and reduce most common hour usage.'
      WHEN ri.risk_level = 'LOW'
        THEN 'LOW PRIORITY: Continue current diversity but avoid consecutive same-hour sends.'
      ELSE 'GOOD: Maintain current time variance patterns.'
    END AS improvement_strategy,
    
    CURRENT_TIMESTAMP() AS calculated_at
    
  FROM risk_info ri
  JOIN page_constraints pc USING (username_std)
)

SELECT
  s.username_page,
  s.username_std,
  s.risk_level,
  s.overall_risk_score,
  s.improvement_strategy,
  
  -- Current state
  s.unique_hours_used AS current_hours_count,
  s.most_common_hour AS overused_hour,
  s.current_active_hours,
  
  -- Constraints
  s.min_allowed_hour,
  s.max_allowed_hour,
  
  -- Suggestions
  s.suggested_hours,
  s.unused_hours_in_range,
  
  -- Quick action items
  CASE 
    WHEN s.risk_level IN ('CRITICAL', 'HIGH') THEN
      ARRAY(
        SELECT CONCAT('Avoid hour ', CAST(s.most_common_hour AS STRING), ' for next 3 sends')
        UNION ALL SELECT 'Add hour ' || CAST(hour AS STRING) || ' to rotation'
        FROM UNNEST(s.unused_hours_in_range) AS hour
        LIMIT 3
      )
    WHEN s.risk_level = 'MEDIUM' THEN
      ARRAY(
        SELECT 'Try hour ' || CAST(hour AS STRING) || ' this week'
        FROM UNNEST(s.unused_hours_in_range) AS hour  
        LIMIT 2
      )
    ELSE ['Continue current patterns with minor adjustments']
  END AS quick_actions,
  
  s.calculated_at

FROM suggestions s
WHERE s.overall_risk_score > 10  -- Only show pages that need improvement
ORDER BY s.overall_risk_score DESC, s.username_page;