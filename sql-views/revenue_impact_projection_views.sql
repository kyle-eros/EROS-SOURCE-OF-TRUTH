-- =====================================================
-- REVENUE IMPACT PROJECTION VIEWS
-- =====================================================
-- This file contains views for projecting revenue impact of scheduling decisions
-- and helping schedulers prioritize high-value opportunities.

-- Core Revenue Impact Analysis
-- Projects revenue based on current recommendations vs historical performance
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_revenue_impact_projections` AS
WITH
-- Current slot recommendations with pricing
current_recommendations AS (
  SELECT
    sr.username_page,
    sr.slot_dt_local,
    sr.dow,
    sr.hod,
    sr.reco_dm_type,
    sr.reco_price_usd,
    sr.best_ppv_buy_rate,
    sr.rps_lcb,
    
    -- Expected revenue from current recommendation
    sr.reco_price_usd * COALESCE(sr.best_ppv_buy_rate, 0.08) AS projected_revenue_current
    
  FROM `of-scheduler-proj.mart.v_slot_recommendations_next24_v3` sr
),

-- Historical performance for same DOW/HOD combinations
historical_baseline AS (
  SELECT
    username_page,
    dow,
    hod,
    COUNT(*) AS historical_sends,
    AVG(price_usd) AS avg_historical_price,
    AVG(SAFE_DIVIDE(purchased, NULLIF(sent, 0))) AS avg_historical_conversion,
    AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS avg_historical_rps,
    AVG(earnings_usd) AS avg_historical_revenue,
    STDDEV(earnings_usd) AS revenue_volatility,
    
    -- Performance trend (last 30 vs 30-90 days)
    AVG(CASE WHEN sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
             THEN SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0)) END) AS recent_rps,
    AVG(CASE WHEN sending_ts < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
             THEN SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0)) END) AS older_rps
             
  FROM `of-scheduler-proj.core.v_message_facts_by_page`
  WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND sent > 0
    AND EXTRACT(DAYOFWEEK FROM sending_ts) IS NOT NULL
  GROUP BY username_page, EXTRACT(DAYOFWEEK FROM sending_ts), EXTRACT(HOUR FROM sending_ts)
  HAVING COUNT(*) >= 2  -- Minimum historical data requirement
),

-- Alternative pricing analysis (what if we used different prices?)
price_alternatives AS (
  SELECT
    pc.username_page,
    pc.dow, 
    pc.hod,
    pc.price_q AS alt_price,
    pc.sent_sum,
    pc.purchased_sum,
    pc.p_buy_eb AS alt_conversion_rate,
    pc.price_q * pc.p_buy_eb AS alt_projected_rps
  FROM `of-scheduler-proj.mart.v_ppv_price_curve_28d_v3` pc
),

-- Best alternative price for each slot
best_alternatives AS (
  SELECT
    pa.username_page,
    pa.dow,
    pa.hod,
    pa.alt_price AS best_alt_price,
    pa.alt_conversion_rate AS best_alt_conversion,
    pa.alt_projected_rps AS best_alt_rps,
    ROW_NUMBER() OVER (
      PARTITION BY pa.username_page, pa.dow, pa.hod 
      ORDER BY pa.alt_projected_rps DESC
    ) AS rn
  FROM price_alternatives pa
),

-- Volume impact analysis
volume_context AS (
  SELECT
    vr.username_page,
    vr.recommended_daily_sends,
    vr.avg_conversion_rate AS volume_avg_conversion,
    vr.tier,
    vr.page_state,
    vr.volume_reasoning
  FROM `of-scheduler-proj.core.v_ppv_volume_recommendations` vr
),

-- Caption performance context
caption_context AS (
  SELECT
    cr.username_page,
    cr.slot_dt_local,
    cr.caption_id,
    cr.p_buy_eb AS caption_conversion_rate,
    cr.rps_eb_price AS caption_rps,
    cr.style_score,
    
    -- Best caption for this slot
    ROW_NUMBER() OVER (
      PARTITION BY cr.username_page, cr.slot_dt_local 
      ORDER BY cr.score_final DESC
    ) AS caption_rank
  FROM `of-scheduler-proj.mart.v_caption_rank_next24_v3` cr
),

-- Main revenue impact calculation
revenue_calculations AS (
  SELECT
    cr.username_page,
    REGEXP_EXTRACT(cr.username_page, r'^([^_]+)') AS username_std,
    cr.slot_dt_local,
    cr.dow,
    cr.hod,
    cr.reco_dm_type,
    
    -- Current recommendation
    cr.reco_price_usd AS current_price,
    cr.best_ppv_buy_rate AS current_conversion_rate,
    cr.projected_revenue_current,
    
    -- Historical baseline
    COALESCE(hb.avg_historical_price, 0) AS historical_price,
    COALESCE(hb.avg_historical_conversion, 0.08) AS historical_conversion,
    COALESCE(hb.avg_historical_revenue, 0) AS historical_revenue,
    COALESCE(hb.historical_sends, 0) AS historical_sample_size,
    
    -- Best alternative pricing
    COALESCE(ba.best_alt_price, cr.reco_price_usd) AS best_alt_price,
    COALESCE(ba.best_alt_conversion, cr.best_ppv_buy_rate) AS best_alt_conversion,
    COALESCE(ba.best_alt_rps, cr.projected_revenue_current) AS best_alt_revenue,
    
    -- Volume context
    COALESCE(vc.recommended_daily_sends, 4) AS recommended_volume,
    vc.tier,
    vc.page_state,
    
    -- Caption context (best caption performance)
    COALESCE(cc.caption_rps, cr.projected_revenue_current) AS best_caption_revenue,
    cc.style_score,
    
    -- Trend indicators
    COALESCE(hb.recent_rps, 0) AS recent_performance,
    COALESCE(hb.older_rps, 0) AS older_performance,
    SAFE_DIVIDE(COALESCE(hb.recent_rps, 0), NULLIF(COALESCE(hb.older_rps, 1), 0)) AS performance_trend_ratio,
    
    -- Risk indicators
    COALESCE(hb.revenue_volatility, 0) AS revenue_volatility
    
  FROM current_recommendations cr
  LEFT JOIN historical_baseline hb USING (username_page, dow, hod)
  LEFT JOIN best_alternatives ba ON ba.username_page = cr.username_page 
                                  AND ba.dow = cr.dow 
                                  AND ba.hod = cr.hod 
                                  AND ba.rn = 1
  LEFT JOIN volume_context vc USING (username_page)
  LEFT JOIN caption_context cc ON cc.username_page = cr.username_page 
                                AND cc.slot_dt_local = cr.slot_dt_local 
                                AND cc.caption_rank = 1
),

-- Calculate all impact scenarios
impact_scenarios AS (
  SELECT
    rc.*,
    
    -- Revenue impact vs historical baseline
    rc.projected_revenue_current - rc.historical_revenue AS revenue_lift_vs_historical,
    SAFE_DIVIDE(
      rc.projected_revenue_current - rc.historical_revenue, 
      NULLIF(rc.historical_revenue, 0)
    ) AS revenue_lift_pct_vs_historical,
    
    -- Revenue impact vs alternative pricing
    rc.best_alt_revenue - rc.projected_revenue_current AS revenue_opportunity_alt_price,
    SAFE_DIVIDE(
      rc.best_alt_revenue - rc.projected_revenue_current,
      NULLIF(rc.projected_revenue_current, 0)
    ) AS revenue_opportunity_pct_alt_price,
    
    -- Caption optimization opportunity
    rc.best_caption_revenue - rc.projected_revenue_current AS revenue_opportunity_caption,
    
    -- Volume-adjusted projections (daily impact)
    rc.projected_revenue_current * rc.recommended_volume AS daily_revenue_projection,
    rc.historical_revenue * GREATEST(rc.recommended_volume, 1) AS daily_historical_baseline,
    
    -- Confidence scoring based on historical data
    CASE
      WHEN rc.historical_sample_size >= 10 THEN 'HIGH'
      WHEN rc.historical_sample_size >= 5 THEN 'MEDIUM'
      WHEN rc.historical_sample_size >= 2 THEN 'LOW'
      ELSE 'VERY_LOW'
    END AS projection_confidence,
    
    -- Risk assessment
    CASE
      WHEN rc.revenue_volatility > rc.projected_revenue_current * 0.5 THEN 'HIGH_VOLATILITY'
      WHEN rc.revenue_volatility > rc.projected_revenue_current * 0.3 THEN 'MEDIUM_VOLATILITY'
      ELSE 'STABLE'
    END AS revenue_risk_level,
    
    CURRENT_TIMESTAMP() AS calculated_at
    
  FROM revenue_calculations rc
)

SELECT
  ist.username_page,
  ist.username_std,
  ist.slot_dt_local,
  FORMAT_DATETIME('%a %b %d, %I:%M %p', ist.slot_dt_local) AS slot_display,
  ist.dow,
  ist.hod,
  ist.reco_dm_type,
  
  -- Current recommendation details
  ist.current_price,
  ist.current_conversion_rate,
  ist.projected_revenue_current,
  
  -- Impact analysis
  ist.revenue_lift_vs_historical,
  ist.revenue_lift_pct_vs_historical,
  ist.revenue_opportunity_alt_price,
  ist.revenue_opportunity_pct_alt_price,
  ist.revenue_opportunity_caption,
  
  -- Volume-adjusted projections
  ist.daily_revenue_projection,
  ist.daily_historical_baseline,
  ist.daily_revenue_projection - ist.daily_historical_baseline AS daily_revenue_lift,
  
  -- Alternative recommendations
  ist.best_alt_price,
  ist.best_alt_conversion,
  ist.best_alt_revenue,
  
  -- Context
  ist.historical_price,
  ist.historical_conversion,
  ist.historical_revenue,
  ist.historical_sample_size,
  ist.recommended_volume,
  ist.tier,
  ist.page_state,
  
  -- Performance indicators
  ist.performance_trend_ratio,
  CASE
    WHEN ist.performance_trend_ratio > 1.2 THEN 'IMPROVING'
    WHEN ist.performance_trend_ratio > 1.05 THEN 'STABLE_UP'
    WHEN ist.performance_trend_ratio < 0.8 THEN 'DECLINING'
    WHEN ist.performance_trend_ratio < 0.95 THEN 'STABLE_DOWN'
    ELSE 'STABLE'
  END AS performance_trend,
  
  -- Priority scoring for UI
  COALESCE(ist.revenue_lift_vs_historical, 0) + 
  COALESCE(ist.revenue_opportunity_alt_price, 0) * 0.5 +
  COALESCE(ist.revenue_opportunity_caption, 0) * 0.3 AS total_opportunity_score,
  
  -- Confidence and risk
  ist.projection_confidence,
  ist.revenue_risk_level,
  ist.style_score,
  
  ist.calculated_at

FROM impact_scenarios ist
ORDER BY total_opportunity_score DESC, ist.slot_dt_local;


-- Daily Revenue Impact Summary
-- Aggregates revenue opportunities by page for daily planning
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_daily_revenue_impact_summary` AS
WITH
-- Daily aggregations from slot-level projections
daily_aggregates AS (
  SELECT
    username_page,
    username_std,
    DATE(slot_dt_local) AS projection_date,
    COUNT(*) AS recommended_slots,
    
    -- Revenue projections
    SUM(projected_revenue_current) AS total_projected_revenue,
    SUM(daily_historical_baseline) AS total_historical_baseline,
    SUM(daily_revenue_lift) AS total_daily_lift,
    
    -- Opportunities
    SUM(GREATEST(revenue_opportunity_alt_price, 0)) AS total_pricing_opportunity,
    SUM(GREATEST(revenue_opportunity_caption, 0)) AS total_caption_opportunity,
    
    -- Volume context
    MAX(recommended_volume) AS daily_volume_recommendation,
    AVG(current_price) AS avg_recommended_price,
    AVG(current_conversion_rate) AS avg_projected_conversion,
    
    -- Performance distribution
    COUNTIF(performance_trend = 'IMPROVING') AS improving_slots,
    COUNTIF(performance_trend = 'DECLINING') AS declining_slots,
    COUNTIF(projection_confidence = 'HIGH') AS high_confidence_slots,
    
    -- Risk assessment
    COUNTIF(revenue_risk_level = 'HIGH_VOLATILITY') AS high_risk_slots,
    
    MAX(total_opportunity_score) AS max_slot_opportunity,
    AVG(total_opportunity_score) AS avg_slot_opportunity
    
  FROM `of-scheduler-proj.core.v_revenue_impact_projections`
  GROUP BY username_page, username_std, DATE(slot_dt_local)
),

-- Page context for prioritization
page_context AS (
  SELECT
    username_std,
    tier,
    page_state,
    COALESCE(active_fans, 0) AS active_fans,
    COALESCE(rev_7d, 0) AS recent_revenue_7d,
    COALESCE(rev_28d, 0) AS recent_revenue_28d
  FROM `of-scheduler-proj.core.page_state` ps
  LEFT JOIN `of-scheduler-proj.staging.creator_stats_latest` cs USING (username_std)
)

SELECT
  da.username_page,
  da.username_std,
  da.projection_date,
  da.recommended_slots,
  da.daily_volume_recommendation,
  
  -- Revenue projections
  ROUND(da.total_projected_revenue, 2) AS total_projected_revenue,
  ROUND(da.total_historical_baseline, 2) AS total_historical_baseline,
  ROUND(da.total_daily_lift, 2) AS total_daily_lift,
  ROUND(SAFE_DIVIDE(da.total_daily_lift, NULLIF(da.total_historical_baseline, 0)) * 100, 1) AS daily_lift_percentage,
  
  -- Opportunity analysis
  ROUND(da.total_pricing_opportunity, 2) AS total_pricing_opportunity,
  ROUND(da.total_caption_opportunity, 2) AS total_caption_opportunity,
  ROUND(da.total_pricing_opportunity + da.total_caption_opportunity, 2) AS total_optimization_opportunity,
  
  -- Performance summary
  da.avg_recommended_price,
  ROUND(da.avg_projected_conversion, 4) AS avg_projected_conversion,
  da.improving_slots,
  da.declining_slots,
  da.high_confidence_slots,
  da.high_risk_slots,
  
  -- Page context
  pc.tier,
  pc.page_state,
  pc.active_fans,
  ROUND(pc.recent_revenue_7d, 2) AS recent_revenue_7d,
  
  -- Priority scoring (for scheduler workload management)
  (da.total_daily_lift * 0.4 +
   da.total_optimization_opportunity * 0.3 +
   da.avg_slot_opportunity * da.recommended_slots * 0.2 +
   CASE pc.tier
     WHEN 'top_tier' THEN 10
     WHEN 'high_performance' THEN 7
     WHEN 'standard' THEN 5
     ELSE 3
   END * 0.1) AS priority_score,
   
  -- Recommendations
  CASE
    WHEN da.total_daily_lift > 50 AND da.high_confidence_slots >= da.recommended_slots * 0.7 
      THEN 'HIGH_IMPACT: Strong revenue opportunity with high confidence'
    WHEN da.total_optimization_opportunity > 30 
      THEN 'OPTIMIZATION: Significant pricing/caption improvements available' 
    WHEN da.declining_slots > da.improving_slots AND da.declining_slots > 2
      THEN 'ATTENTION: Multiple declining performance slots need review'
    WHEN da.high_risk_slots > da.recommended_slots * 0.5
      THEN 'CAUTION: High volatility - monitor performance closely'
    WHEN da.total_daily_lift > 0 
      THEN 'POSITIVE: Moderate improvement over historical performance'
    ELSE 'STABLE: Continue current approach'
  END AS daily_recommendation,
  
  -- Action items
  ARRAY(
    SELECT action FROM UNNEST([
      CASE WHEN da.total_pricing_opportunity > 20 
           THEN 'Review pricing: $' || CAST(ROUND(da.total_pricing_opportunity, 0) AS STRING) || ' opportunity'
      END,
      CASE WHEN da.total_caption_opportunity > 15
           THEN 'Optimize captions: $' || CAST(ROUND(da.total_caption_opportunity, 0) AS STRING) || ' potential'
      END,
      CASE WHEN da.declining_slots >= 2
           THEN 'Address ' || CAST(da.declining_slots AS STRING) || ' declining time slots'
      END,
      CASE WHEN da.high_risk_slots >= 2
           THEN 'Monitor ' || CAST(da.high_risk_slots AS STRING) || ' volatile slots'
      END,
      CASE WHEN da.recommended_slots < da.daily_volume_recommendation
           THEN 'Increase volume: ' || CAST(da.daily_volume_recommendation - da.recommended_slots AS STRING) || ' more slots needed'
      END
    ]) AS action
    WHERE action IS NOT NULL
  ) AS action_items,
  
  CURRENT_TIMESTAMP() AS calculated_at

FROM daily_aggregates da
JOIN page_context pc USING (username_std)
ORDER BY priority_score DESC, da.projection_date, da.username_page;


-- Revenue Opportunity Ranking
-- Ranks individual slots by revenue opportunity for priority scheduling
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_revenue_opportunity_ranking` AS
WITH
-- Enhanced slot scoring
slot_scoring AS (
  SELECT
    rip.*,
    
    -- Composite opportunity score
    COALESCE(rip.revenue_lift_vs_historical, 0) * 0.4 +
    COALESCE(rip.revenue_opportunity_alt_price, 0) * 0.3 +
    COALESCE(rip.revenue_opportunity_caption, 0) * 0.2 +
    (rip.projected_revenue_current * CASE rip.tier
       WHEN 'top_tier' THEN 1.2
       WHEN 'high_performance' THEN 1.1
       ELSE 1.0
     END) * 0.1 AS composite_opportunity_score,
    
    -- Confidence-adjusted score
    CASE rip.projection_confidence
      WHEN 'HIGH' THEN 1.0
      WHEN 'MEDIUM' THEN 0.8
      WHEN 'LOW' THEN 0.6
      ELSE 0.4
    END AS confidence_multiplier,
    
    -- Risk adjustment
    CASE rip.revenue_risk_level
      WHEN 'HIGH_VOLATILITY' THEN 0.7
      WHEN 'MEDIUM_VOLATILITY' THEN 0.85
      ELSE 1.0
    END AS risk_adjustment
  FROM `of-scheduler-proj.core.v_revenue_impact_projections` rip
)

SELECT
  ss.username_page,
  ss.username_std,
  ss.slot_dt_local,
  ss.slot_display,
  ss.dow,
  ss.hod,
  
  -- Opportunity metrics
  ROUND(ss.projected_revenue_current, 2) AS projected_revenue,
  ROUND(ss.revenue_lift_vs_historical, 2) AS vs_historical_lift,
  ROUND(ss.revenue_opportunity_alt_price, 2) AS pricing_opportunity,
  ROUND(ss.revenue_opportunity_caption, 2) AS caption_opportunity,
  
  -- Scoring
  ROUND(ss.composite_opportunity_score, 2) AS raw_opportunity_score,
  ROUND(ss.composite_opportunity_score * ss.confidence_multiplier * ss.risk_adjustment, 2) AS adjusted_opportunity_score,
  
  -- Rankings
  ROW_NUMBER() OVER (ORDER BY ss.composite_opportunity_score * ss.confidence_multiplier * ss.risk_adjustment DESC) AS global_rank,
  ROW_NUMBER() OVER (PARTITION BY ss.username_page ORDER BY ss.composite_opportunity_score * ss.confidence_multiplier * ss.risk_adjustment DESC) AS page_rank,
  ROW_NUMBER() OVER (PARTITION BY DATE(ss.slot_dt_local) ORDER BY ss.composite_opportunity_score * ss.confidence_multiplier * ss.risk_adjustment DESC) AS daily_rank,
  
  -- Context
  ss.tier,
  ss.page_state,
  ss.performance_trend,
  ss.projection_confidence,
  ss.revenue_risk_level,
  
  -- Recommendations
  CASE
    WHEN ROW_NUMBER() OVER (ORDER BY ss.composite_opportunity_score * ss.confidence_multiplier * ss.risk_adjustment DESC) <= 10
      THEN 'TOP_PRIORITY: Schedule first - highest revenue impact'
    WHEN ROW_NUMBER() OVER (PARTITION BY ss.username_page ORDER BY ss.composite_opportunity_score * ss.confidence_multiplier * ss.risk_adjustment DESC) = 1
      THEN 'PAGE_PRIORITY: Best slot for this creator'
    WHEN ss.revenue_opportunity_alt_price > 20
      THEN 'PRICING_FOCUS: Consider alternative pricing'
    WHEN ss.revenue_opportunity_caption > 15
      THEN 'CAPTION_FOCUS: Optimize caption selection'
    WHEN ss.composite_opportunity_score > 10
      THEN 'GOOD_OPPORTUNITY: Above average potential'
    ELSE 'STANDARD: Maintain current approach'
  END AS scheduling_priority,
  
  ss.calculated_at

FROM slot_scoring ss
WHERE ss.composite_opportunity_score > 0
ORDER BY adjusted_opportunity_score DESC, ss.slot_dt_local;