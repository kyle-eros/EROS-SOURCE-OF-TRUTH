-- =====================================================
-- INTEGRATED ML SCHEDULING ENGINE WITH OPTIMIZATIONS
-- =====================================================
-- This file integrates all optimization views with the existing ML recommendation
-- engine to create an enhanced 7-day scheduling system.

-- Enhanced Slot Recommendations with All Optimizations
-- Integrates volume, time variance, and revenue impact optimizations
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_enhanced_slot_recommendations_next24` AS
WITH
-- Base slot recommendations from existing system
base_slots AS (
  SELECT
    sr.*,
    -- Add DOW for time variance analysis
    MOD(EXTRACT(DAYOFWEEK FROM sr.slot_dt_local) + 5, 7) AS dow
  FROM `of-scheduler-proj.mart.v_slot_recommendations_next24_v3` sr
),

-- Volume optimization integration
volume_enhanced AS (
  SELECT
    bs.*,
    vr.recommended_daily_sends,
    vr.volume_reasoning,
    vr.recommendation_confidence AS volume_confidence,
    
    -- Volume-based slot scoring adjustment
    CASE 
      WHEN vr.recommended_daily_sends >= 8 THEN 1.2  -- High volume pages get boost
      WHEN vr.recommended_daily_sends >= 6 THEN 1.1
      WHEN vr.recommended_daily_sends <= 3 THEN 0.9  -- Conservative volume pages
      ELSE 1.0
    END AS volume_score_multiplier
    
  FROM base_slots bs
  LEFT JOIN `of-scheduler-proj.core.v_ppv_volume_recommendations` vr USING (username_page)
),

-- Time variance integration
time_variance_enhanced AS (
  SELECT
    ve.*,
    
    -- Get time variance risk for this page
    COALESCE(tvd.overall_risk_score, 0) AS time_variance_risk,
    COALESCE(tvd.risk_level, 'MINIMAL') AS time_variance_level,
    
    -- Check if this specific hour contributes to robotic patterns
    CASE
      WHEN tvd.most_common_hour = ve.hod AND tvd.most_common_hour_ratio > 0.4 
        THEN 0.7  -- Penalize overused hours
      WHEN tvd.overall_risk_score > 50 AND ve.hod IN UNNEST(SPLIT(CAST(tvd.most_common_hour AS STRING), ','))
        THEN 0.8  -- Moderate penalty for high-risk pages
      WHEN tvd.overall_risk_score < 20 
        THEN 1.1  -- Reward good variance
      ELSE 1.0
    END AS time_variance_multiplier,
    
    -- Time variance alerts for this slot
    ARRAY(
      SELECT STRUCT(
        alert_type,
        severity,
        message
      )
      FROM `of-scheduler-proj.core.v_time_variance_alerts` tva
      WHERE tva.username_page = ve.username_page
      ORDER BY alert_priority_score DESC
      LIMIT 3
    ) AS time_variance_alerts
    
  FROM volume_enhanced ve
  LEFT JOIN `of-scheduler-proj.core.v_time_variance_detection` tvd USING (username_page)
),

-- Revenue impact integration
revenue_enhanced AS (
  SELECT
    tve.*,
    
    -- Revenue impact projections
    rip.projected_revenue_current,
    rip.revenue_lift_vs_historical,
    rip.revenue_opportunity_alt_price,
    rip.revenue_opportunity_caption,
    rip.total_opportunity_score,
    rip.projection_confidence,
    rip.performance_trend,
    
    -- Revenue-based scoring adjustment
    CASE
      WHEN rip.total_opportunity_score > 50 THEN 1.3  -- High opportunity slots
      WHEN rip.total_opportunity_score > 25 THEN 1.15
      WHEN rip.total_opportunity_score > 10 THEN 1.05
      WHEN rip.revenue_lift_vs_historical < -5 THEN 0.8  -- Negative opportunity
      ELSE 1.0
    END AS revenue_opportunity_multiplier
    
  FROM time_variance_enhanced tve
  LEFT JOIN `of-scheduler-proj.core.v_revenue_impact_projections` rip 
    ON rip.username_page = tve.username_page 
    AND rip.slot_dt_local = tve.slot_dt_local
),

-- Calculate enhanced composite scores
enhanced_scoring AS (
  SELECT
    re.*,
    
    -- Original slot score (from existing system)
    re.slot_score_base AS original_slot_score,
    
    -- Enhanced composite score with all optimizations
    re.slot_score_base *
    re.volume_score_multiplier *
    re.time_variance_multiplier *
    re.revenue_opportunity_multiplier AS enhanced_slot_score,
    
    -- Individual optimization contributions
    re.slot_score_base * (re.volume_score_multiplier - 1) AS volume_contribution,
    re.slot_score_base * (re.time_variance_multiplier - 1) AS time_variance_contribution,
    re.slot_score_base * (re.revenue_opportunity_multiplier - 1) AS revenue_contribution,
    
    -- Total optimization impact
    (re.volume_score_multiplier * re.time_variance_multiplier * re.revenue_opportunity_multiplier) - 1 AS total_optimization_impact
    
  FROM revenue_enhanced re
),

-- Apply volume constraints and final ranking
final_recommendations AS (
  SELECT
    es.*,
    
    -- Rank slots by enhanced score within each page
    ROW_NUMBER() OVER (
      PARTITION BY es.username_page, DATE(es.slot_dt_local)
      ORDER BY es.enhanced_slot_score DESC, es.slot_dt_local
    ) AS enhanced_daily_rank,
    
    -- Apply volume constraints
    CASE 
      WHEN ROW_NUMBER() OVER (
        PARTITION BY es.username_page, DATE(es.slot_dt_local)
        ORDER BY es.enhanced_slot_score DESC, es.slot_dt_local
      ) <= COALESCE(es.recommended_daily_sends, 4)
      THEN TRUE
      ELSE FALSE
    END AS within_volume_recommendation,
    
    -- Generate recommendation explanation
    CONCAT(
      'Base: ', CAST(ROUND(es.original_slot_score, 3) AS STRING),
      ' | Enhanced: ', CAST(ROUND(es.enhanced_slot_score, 3) AS STRING),
      ' | Impact: +', CAST(ROUND(es.total_optimization_impact * 100, 1) AS STRING), '%',
      CASE WHEN es.volume_contribution != 0 THEN CONCAT(' (Vol:', CAST(ROUND(es.volume_contribution, 2) AS STRING), ')') ELSE '' END,
      CASE WHEN es.time_variance_contribution != 0 THEN CONCAT(' (TV:', CAST(ROUND(es.time_variance_contribution, 2) AS STRING), ')') ELSE '' END,
      CASE WHEN es.revenue_contribution != 0 THEN CONCAT(' (Rev:', CAST(ROUND(es.revenue_contribution, 2) AS STRING), ')') ELSE '' END
    ) AS optimization_explanation
    
  FROM enhanced_scoring es
)

SELECT
  fr.username_page,
  fr.slot_dt_local,
  FORMAT_DATETIME('%a %b %d, %I:%M %p', fr.slot_dt_local) AS slot_display,
  fr.dow,
  fr.hod,
  fr.reco_dm_type,
  fr.reco_price_usd,
  
  -- Enhanced scoring
  ROUND(fr.original_slot_score, 4) AS original_score,
  ROUND(fr.enhanced_slot_score, 4) AS enhanced_score,
  fr.enhanced_daily_rank,
  fr.within_volume_recommendation,
  
  -- Optimization factors
  fr.volume_score_multiplier,
  fr.time_variance_multiplier,
  fr.revenue_opportunity_multiplier,
  ROUND(fr.total_optimization_impact * 100, 1) AS optimization_impact_pct,
  
  -- Volume optimization
  fr.recommended_daily_sends,
  fr.volume_reasoning,
  fr.volume_confidence,
  
  -- Time variance optimization
  fr.time_variance_risk,
  fr.time_variance_level,
  fr.time_variance_alerts,
  
  -- Revenue optimization
  ROUND(fr.projected_revenue_current, 2) AS projected_revenue,
  ROUND(fr.revenue_lift_vs_historical, 2) AS revenue_lift_historical,
  ROUND(fr.revenue_opportunity_alt_price, 2) AS pricing_opportunity,
  ROUND(fr.revenue_opportunity_caption, 2) AS caption_opportunity,
  fr.projection_confidence,
  fr.performance_trend,
  
  -- Original system fields preserved
  fr.best_ppv_buy_rate,
  fr.rps_lcb,
  
  -- Recommendation priority
  CASE
    WHEN fr.enhanced_daily_rank = 1 THEN 'TOP_PRIORITY'
    WHEN fr.within_volume_recommendation AND fr.total_optimization_impact > 0.2 THEN 'HIGH_PRIORITY'
    WHEN fr.within_volume_recommendation THEN 'RECOMMENDED'
    WHEN fr.enhanced_daily_rank <= 8 THEN 'CONSIDER'
    ELSE 'LOW_PRIORITY'
  END AS recommendation_priority,
  
  -- Action items
  ARRAY(
    SELECT action FROM UNNEST([
      CASE WHEN fr.time_variance_risk > 50 THEN 'WARNING: High time variance risk - vary send times' END,
      CASE WHEN fr.revenue_opportunity_alt_price > 20 THEN CONCAT('OPPORTUNITY: +$', CAST(ROUND(fr.revenue_opportunity_alt_price, 0) AS STRING), ' with different pricing') END,
      CASE WHEN fr.revenue_opportunity_caption > 15 THEN CONCAT('OPPORTUNITY: +$', CAST(ROUND(fr.revenue_opportunity_caption, 0) AS STRING), ' with better caption') END,
      CASE WHEN fr.performance_trend = 'DECLINING' THEN 'CAUTION: Performance declining for this time slot' END,
      CASE WHEN fr.volume_confidence = 'VERY_LOW' THEN 'NOTE: Volume recommendation has low confidence' END
    ]) AS action
    WHERE action IS NOT NULL
  ) AS action_items,
  
  fr.optimization_explanation,
  CURRENT_TIMESTAMP() AS calculated_at

FROM final_recommendations fr
ORDER BY fr.username_page, fr.enhanced_daily_rank, fr.slot_dt_local;


-- Enhanced Weekly Template with Optimizations
-- Extends the existing weekly template with all optimization insights
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_enhanced_weekly_template_7d` AS
WITH
-- Get base weekly template
base_template AS (
  SELECT * FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`
),

-- Enhance with optimization data
optimized_template AS (
  SELECT
    bt.*,
    
    -- Volume optimization context
    vr.recommended_daily_sends,
    vr.volume_reasoning,
    vr.tier AS volume_tier,
    vr.page_state,
    
    -- DOW-specific volume recommendations
    vrd.dow_recommended_volume,
    vrd.dow_reasoning,
    vrd.dow_performance_ratio,
    
    -- Time variance risk assessment
    tvd.overall_risk_score AS time_variance_risk,
    tvd.risk_level AS time_variance_level,
    tvd.most_common_hour,
    tvd.most_common_hour_ratio,
    
    -- Check if this slot contributes to robotic patterns
    CASE
      WHEN tvd.most_common_hour = bt.hod_local AND tvd.most_common_hour_ratio > 0.4
        THEN 'HIGH_RISK'
      WHEN tvd.overall_risk_score > 50
        THEN 'MEDIUM_RISK'
      WHEN tvd.overall_risk_score < 20
        THEN 'LOW_RISK'
      ELSE 'NORMAL'
    END AS slot_variance_risk,
    
    -- Revenue projections (if available for this slot time)
    rip.projected_revenue_current,
    rip.revenue_lift_vs_historical,
    rip.total_opportunity_score,
    rip.projection_confidence,
    
    -- Daily revenue summary for context
    dris.total_projected_revenue AS daily_projected_revenue,
    dris.total_daily_lift AS daily_revenue_lift,
    dris.priority_score AS daily_priority_score
    
  FROM base_template bt
  LEFT JOIN `of-scheduler-proj.core.v_ppv_volume_recommendations` vr USING (username_page)
  LEFT JOIN `of-scheduler-proj.core.v_ppv_volume_recommendations_dow` vrd 
    ON vrd.username_page = bt.username_page 
    AND vrd.dow = MOD(EXTRACT(DAYOFWEEK FROM bt.date_local) + 5, 7)
  LEFT JOIN `of-scheduler-proj.core.v_time_variance_detection` tvd USING (username_page)
  LEFT JOIN `of-scheduler-proj.core.v_revenue_impact_projections` rip
    ON rip.username_page = bt.username_page
    AND EXTRACT(HOUR FROM rip.slot_dt_local) = bt.hod_local
    AND DATE(rip.slot_dt_local) = bt.date_local
  LEFT JOIN `of-scheduler-proj.core.v_daily_revenue_impact_summary` dris
    ON dris.username_page = bt.username_page
    AND dris.projection_date = bt.date_local
),

-- Calculate optimization scores and recommendations
final_template AS (
  SELECT
    ot.*,
    
    -- Volume compliance scoring
    CASE
      WHEN ot.slot_rank <= COALESCE(ot.dow_recommended_volume, ot.recommended_daily_sends, 4) THEN 1.0
      WHEN ot.slot_rank <= COALESCE(ot.dow_recommended_volume, ot.recommended_daily_sends, 4) + 1 THEN 0.8
      ELSE 0.5
    END AS volume_compliance_score,
    
    -- Time variance health score  
    CASE ot.slot_variance_risk
      WHEN 'LOW_RISK' THEN 1.0
      WHEN 'NORMAL' THEN 0.9
      WHEN 'MEDIUM_RISK' THEN 0.7
      WHEN 'HIGH_RISK' THEN 0.4
      ELSE 0.8
    END AS time_variance_score,
    
    -- Revenue opportunity score
    CASE
      WHEN ot.total_opportunity_score > 50 THEN 1.2
      WHEN ot.total_opportunity_score > 25 THEN 1.1
      WHEN ot.total_opportunity_score > 10 THEN 1.0
      WHEN ot.revenue_lift_vs_historical < -5 THEN 0.7
      ELSE 0.9
    END AS revenue_opportunity_score,
    
    -- Overall optimization health
    (CASE
       WHEN ot.slot_rank <= COALESCE(ot.dow_recommended_volume, ot.recommended_daily_sends, 4) THEN 1.0
       ELSE 0.7
     END * 0.4 +
     CASE ot.slot_variance_risk
       WHEN 'LOW_RISK' THEN 1.0
       WHEN 'NORMAL' THEN 0.9
       WHEN 'MEDIUM_RISK' THEN 0.7
       WHEN 'HIGH_RISK' THEN 0.4
       ELSE 0.8
     END * 0.3 +
     CASE
       WHEN ot.total_opportunity_score > 25 THEN 1.0
       WHEN ot.total_opportunity_score > 10 THEN 0.9
       WHEN ot.revenue_lift_vs_historical >= 0 THEN 0.8
       ELSE 0.6
     END * 0.3) AS optimization_health_score
  FROM optimized_template ot
)

SELECT
  ft.username_std,
  ft.page_type,
  ft.username_page,
  ft.scheduler_name,
  ft.tz,
  ft.date_local,
  ft.slot_rank,
  ft.hod_local,
  ft.price_usd,
  ft.planned_local_datetime,
  ft.scheduled_datetime_utc,
  ft.tracking_hash,
  
  -- Optimization metrics
  ROUND(ft.optimization_health_score, 3) AS optimization_health_score,
  ft.volume_compliance_score,
  ft.time_variance_score,
  ft.revenue_opportunity_score,
  
  -- Volume optimization
  ft.recommended_daily_sends,
  ft.dow_recommended_volume,
  ft.volume_reasoning,
  ft.dow_reasoning,
  ft.volume_tier,
  ft.page_state,
  
  -- Time variance optimization
  ft.time_variance_risk,
  ft.time_variance_level,
  ft.slot_variance_risk,
  ft.most_common_hour,
  ft.most_common_hour_ratio,
  
  -- Revenue optimization
  ROUND(ft.projected_revenue_current, 2) AS projected_revenue,
  ROUND(ft.revenue_lift_vs_historical, 2) AS revenue_lift_historical,
  ROUND(ft.total_opportunity_score, 2) AS total_opportunity_score,
  ft.projection_confidence,
  ROUND(ft.daily_projected_revenue, 2) AS daily_projected_revenue,
  ROUND(ft.daily_revenue_lift, 2) AS daily_revenue_lift,
  ft.daily_priority_score,
  
  -- Optimization recommendations
  CASE
    WHEN ft.optimization_health_score >= 0.9 THEN 'EXCELLENT'
    WHEN ft.optimization_health_score >= 0.8 THEN 'GOOD'
    WHEN ft.optimization_health_score >= 0.7 THEN 'FAIR'
    WHEN ft.optimization_health_score >= 0.6 THEN 'NEEDS_IMPROVEMENT'
    ELSE 'POOR'
  END AS optimization_grade,
  
  -- Specific recommendations
  CASE
    WHEN ft.slot_variance_risk = 'HIGH_RISK' THEN 'URGENT: Change this time slot to reduce robotic patterns'
    WHEN ft.volume_compliance_score < 0.8 THEN 'Consider removing this slot - exceeds optimal volume'
    WHEN ft.total_opportunity_score > 50 THEN 'HIGH OPPORTUNITY: Optimize pricing and captions for this slot'
    WHEN ft.revenue_lift_vs_historical < -10 THEN 'UNDERPERFORMING: Review this time slot performance'
    WHEN ft.optimization_health_score >= 0.9 THEN 'OPTIMAL: Continue current approach'
    ELSE 'REVIEW: Minor optimizations available'
  END AS slot_recommendation,
  
  -- Action items for schedulers
  ARRAY(
    SELECT action FROM UNNEST([
      CASE WHEN ft.slot_variance_risk = 'HIGH_RISK' 
           THEN CONCAT('URGENT: Avoid hour ', CAST(ft.hod_local AS STRING), ' - overused (', 
                      CAST(ROUND(ft.most_common_hour_ratio * 100, 0) AS STRING), '% of sends)')
      END,
      CASE WHEN ft.volume_compliance_score < 0.8 
           THEN CONCAT('Volume: Consider removing slot (rank ', CAST(ft.slot_rank AS STRING), 
                      ' exceeds recommended ', CAST(ft.dow_recommended_volume AS STRING), ')')
      END,
      CASE WHEN ft.total_opportunity_score > 30 
           THEN CONCAT('Revenue: $', CAST(ROUND(ft.total_opportunity_score, 0) AS STRING), 
                      ' optimization opportunity')
      END,
      CASE WHEN ft.revenue_lift_vs_historical < -5 
           THEN CONCAT('Performance: -$', CAST(ROUND(ABS(ft.revenue_lift_vs_historical), 0) AS STRING), 
                      ' vs historical')
      END
    ]) AS action
    WHERE action IS NOT NULL
  ) AS optimization_actions,
  
  CURRENT_TIMESTAMP() AS calculated_at

FROM final_template ft
ORDER BY ft.username_page, ft.date_local, ft.slot_rank;


-- Master Optimization Dashboard
-- Provides high-level view of all optimization opportunities across the system
CREATE OR REPLACE VIEW `of-scheduler-proj.core.v_optimization_dashboard` AS
WITH
-- Page-level optimization summary
page_summary AS (
  SELECT
    username_page,
    username_std,
    
    -- Volume optimization status
    AVG(volume_compliance_score) AS avg_volume_compliance,
    COUNT(CASE WHEN volume_compliance_score < 0.8 THEN 1 END) AS volume_issues,
    
    -- Time variance status
    AVG(time_variance_score) AS avg_time_variance_score,
    COUNT(CASE WHEN slot_variance_risk IN ('HIGH_RISK', 'MEDIUM_RISK') THEN 1 END) AS variance_issues,
    
    -- Revenue optimization status
    AVG(revenue_opportunity_score) AS avg_revenue_score,
    SUM(COALESCE(total_opportunity_score, 0)) AS total_revenue_opportunity,
    
    -- Overall health
    AVG(optimization_health_score) AS avg_optimization_health,
    COUNT(*) AS total_slots,
    COUNT(CASE WHEN optimization_health_score < 0.7 THEN 1 END) AS problematic_slots
    
  FROM `of-scheduler-proj.core.v_enhanced_weekly_template_7d`
  GROUP BY username_page, username_std
),

-- System-wide metrics
system_metrics AS (
  SELECT
    COUNT(DISTINCT username_page) AS total_pages,
    AVG(avg_optimization_health) AS system_avg_health,
    SUM(volume_issues) AS total_volume_issues,
    SUM(variance_issues) AS total_variance_issues,
    SUM(total_revenue_opportunity) AS system_revenue_opportunity,
    SUM(problematic_slots) AS total_problematic_slots,
    SUM(total_slots) AS total_system_slots
  FROM page_summary
)

SELECT
  -- System overview
  sm.total_pages,
  ROUND(sm.system_avg_health, 3) AS system_health_score,
  sm.total_volume_issues,
  sm.total_variance_issues,
  ROUND(sm.system_revenue_opportunity, 0) AS total_revenue_opportunity_usd,
  sm.total_problematic_slots,
  sm.total_system_slots,
  ROUND(SAFE_DIVIDE(sm.total_problematic_slots, sm.total_system_slots) * 100, 1) AS problematic_slots_pct,
  
  -- Page-level details
  ARRAY(
    SELECT STRUCT(
      ps.username_page,
      ROUND(ps.avg_optimization_health, 3) as health_score,
      ps.volume_issues,
      ps.variance_issues,
      ROUND(ps.total_revenue_opportunity, 0) as revenue_opportunity,
      ps.problematic_slots,
      CASE
        WHEN ps.avg_optimization_health < 0.6 THEN 'CRITICAL'
        WHEN ps.avg_optimization_health < 0.7 THEN 'NEEDS_ATTENTION'
        WHEN ps.avg_optimization_health < 0.8 THEN 'FAIR'
        WHEN ps.avg_optimization_health < 0.9 THEN 'GOOD'
        ELSE 'EXCELLENT'
      END as status
    )
    FROM page_summary ps
    ORDER BY ps.avg_optimization_health ASC, ps.total_revenue_opportunity DESC
    LIMIT 50
  ) AS page_details,
  
  -- Priority recommendations
  CASE
    WHEN sm.system_avg_health < 0.7 THEN 'SYSTEM CRITICAL: Immediate optimization needed across multiple pages'
    WHEN sm.total_variance_issues > sm.total_pages * 0.3 THEN 'TIME VARIANCE CRISIS: Widespread robotic patterns detected'
    WHEN sm.total_volume_issues > sm.total_pages * 0.2 THEN 'VOLUME ISSUES: Many pages exceed optimal send volumes'
    WHEN sm.system_revenue_opportunity > 1000 THEN 'HIGH OPPORTUNITY: Significant revenue optimization potential'
    WHEN sm.system_avg_health > 0.85 THEN 'SYSTEM HEALTHY: Minor optimizations only'
    ELSE 'MODERATE OPTIMIZATION: Standard improvements needed'
  END AS system_status,
  
  -- Action priorities
  ARRAY(
    SELECT action FROM UNNEST([
      CASE WHEN sm.total_variance_issues > 0 
           THEN CONCAT('Address ', CAST(sm.total_variance_issues AS STRING), ' time variance issues immediately')
      END,
      CASE WHEN sm.total_volume_issues > 0 
           THEN CONCAT('Review volume settings for ', CAST(sm.total_volume_issues AS STRING), ' problematic slots')
      END,
      CASE WHEN sm.system_revenue_opportunity > 500 
           THEN CONCAT('Capture $', CAST(ROUND(sm.system_revenue_opportunity, 0) AS STRING), ' in revenue opportunities')
      END,
      CASE WHEN sm.total_problematic_slots > sm.total_system_slots * 0.1 
           THEN CONCAT('Optimize ', CAST(sm.total_problematic_slots AS STRING), ' underperforming slots')
      END
    ]) AS action
    WHERE action IS NOT NULL
  ) AS priority_actions,
  
  CURRENT_TIMESTAMP() AS calculated_at

FROM system_metrics sm;