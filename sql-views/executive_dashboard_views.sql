-- =====================================
-- EROS EXECUTIVE REVENUE DASHBOARD VIEWS
-- =====================================
-- Creates comprehensive BigQuery views for executive reporting
-- Focuses on revenue, scheduler performance, and page metrics

-- =====================================
-- 1. REVENUE OVERVIEW - PRIMARY KPIs
-- =====================================

CREATE OR REPLACE VIEW `of-scheduler-proj.dashboard.v_revenue_overview` AS
WITH daily_metrics AS (
  SELECT
    DATE(sending_ts) as date,
    COUNT(*) as messages_sent,
    SUM(CAST(sent AS INT64)) as total_sent,
    SUM(CAST(viewed AS INT64)) as total_viewed,
    SUM(CAST(purchased AS INT64)) as total_purchased,
    SUM(CAST(earnings_usd AS FLOAT64)) as total_revenue,
    AVG(CAST(price_usd AS FLOAT64)) as avg_price,
    SAFE_DIVIDE(SUM(CAST(viewed AS INT64)), SUM(CAST(sent AS INT64))) as view_rate,
    SAFE_DIVIDE(SUM(CAST(purchased AS INT64)), SUM(CAST(sent AS INT64))) as purchase_rate,
    SAFE_DIVIDE(SUM(CAST(earnings_usd AS FLOAT64)), SUM(CAST(purchased AS INT64))) as avg_order_value
  FROM `of-scheduler-proj.core.message_facts`
  WHERE DATE(sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND earnings_usd IS NOT NULL
  GROUP BY 1
)
SELECT 
  date,
  messages_sent,
  total_sent,
  total_viewed,
  total_purchased,
  ROUND(total_revenue, 2) as total_revenue,
  ROUND(avg_price, 2) as avg_price,
  ROUND(view_rate * 100, 2) as view_rate_pct,
  ROUND(purchase_rate * 100, 2) as purchase_rate_pct,
  ROUND(avg_order_value, 2) as avg_order_value,
  -- Growth metrics
  LAG(total_revenue) OVER (ORDER BY date) as prev_day_revenue,
  ROUND(total_revenue - LAG(total_revenue) OVER (ORDER BY date), 2) as revenue_change,
  ROUND(SAFE_DIVIDE(total_revenue - LAG(total_revenue) OVER (ORDER BY date), LAG(total_revenue) OVER (ORDER BY date)) * 100, 1) as revenue_growth_pct
FROM daily_metrics
ORDER BY date DESC;

-- =====================================
-- 2. SCHEDULER PERFORMANCE RANKING
-- =====================================

CREATE OR REPLACE VIEW `of-scheduler-proj.dashboard.v_scheduler_performance` AS
WITH scheduler_stats AS (
  SELECT
    COALESCE(pso.assigned_scheduler, 'unassigned') as scheduler_name,
    COUNT(DISTINCT mf.username_std) as pages_managed,
    COUNT(*) as messages_sent,
    SUM(CAST(mf.sent AS INT64)) as total_sent,
    SUM(CAST(mf.purchased AS INT64)) as total_purchased,
    SUM(CAST(mf.earnings_usd AS FLOAT64)) as total_revenue,
    SAFE_DIVIDE(SUM(CAST(mf.purchased AS INT64)), SUM(CAST(mf.sent AS INT64))) as conversion_rate,
    SAFE_DIVIDE(SUM(CAST(mf.earnings_usd AS FLOAT64)), COUNT(*)) as revenue_per_message,
    SAFE_DIVIDE(SUM(CAST(mf.earnings_usd AS FLOAT64)), COUNT(DISTINCT mf.username_std)) as revenue_per_page
  FROM `of-scheduler-proj.core.message_facts` mf
  LEFT JOIN `of-scheduler-proj.core.page_scheduler_override` pso
    ON mf.username_std = pso.username_std
  WHERE DATE(mf.sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND mf.earnings_usd IS NOT NULL
  GROUP BY 1
),
scheduler_ranks AS (
  SELECT *,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    ROW_NUMBER() OVER (ORDER BY conversion_rate DESC) as conversion_rank,
    ROW_NUMBER() OVER (ORDER BY revenue_per_message DESC) as efficiency_rank
  FROM scheduler_stats
)
SELECT
  scheduler_name,
  pages_managed,
  messages_sent,
  total_sent,
  total_purchased,
  ROUND(total_revenue, 2) as total_revenue,
  ROUND(conversion_rate * 100, 2) as conversion_rate_pct,
  ROUND(revenue_per_message, 2) as revenue_per_message,
  ROUND(revenue_per_page, 2) as revenue_per_page,
  revenue_rank,
  conversion_rank,
  efficiency_rank,
  -- Overall performance score (weighted average of ranks)
  ROUND((revenue_rank * 0.5 + conversion_rank * 0.3 + efficiency_rank * 0.2), 1) as performance_score
FROM scheduler_ranks
ORDER BY total_revenue DESC;

-- =====================================
-- 3. TOP PERFORMING PAGES LEADERBOARD
-- =====================================

CREATE OR REPLACE VIEW `of-scheduler-proj.dashboard.v_page_performance_leaderboard` AS
WITH page_metrics AS (
  SELECT
    mf.username_std,
    COALESCE(pso.assigned_scheduler, 'unassigned') as scheduler,
    COUNT(*) as messages_sent,
    SUM(CAST(mf.sent AS INT64)) as total_sent,
    SUM(CAST(mf.viewed AS INT64)) as total_viewed,
    SUM(CAST(mf.purchased AS INT64)) as total_purchased,
    SUM(CAST(mf.earnings_usd AS FLOAT64)) as total_revenue,
    AVG(CAST(mf.price_usd AS FLOAT64)) as avg_price,
    SAFE_DIVIDE(SUM(CAST(mf.viewed AS INT64)), SUM(CAST(mf.sent AS INT64))) as view_rate,
    SAFE_DIVIDE(SUM(CAST(mf.purchased AS INT64)), SUM(CAST(mf.sent AS INT64))) as purchase_rate,
    SAFE_DIVIDE(SUM(CAST(mf.earnings_usd AS FLOAT64)), SUM(CAST(mf.sent AS INT64))) as revenue_per_send,
    MAX(DATE(mf.sending_ts)) as last_active_date,
    DATE_DIFF(CURRENT_DATE(), MAX(DATE(mf.sending_ts)), DAY) as days_since_active
  FROM `of-scheduler-proj.core.message_facts` mf
  LEFT JOIN `of-scheduler-proj.core.page_scheduler_override` pso
    ON mf.username_std = pso.username_std
  WHERE DATE(mf.sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND mf.earnings_usd IS NOT NULL
  GROUP BY 1, 2
  HAVING total_sent > 10  -- Filter out low-volume pages
),
page_ranks AS (
  SELECT *,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    ROW_NUMBER() OVER (ORDER BY purchase_rate DESC) as conversion_rank,
    ROW_NUMBER() OVER (ORDER BY revenue_per_send DESC) as efficiency_rank
  FROM page_metrics
)
SELECT
  username_std,
  scheduler,
  messages_sent,
  total_sent,
  total_purchased,
  ROUND(total_revenue, 2) as total_revenue,
  ROUND(avg_price, 2) as avg_price,
  ROUND(view_rate * 100, 2) as view_rate_pct,
  ROUND(purchase_rate * 100, 2) as purchase_rate_pct,
  ROUND(revenue_per_send, 3) as revenue_per_send,
  last_active_date,
  days_since_active,
  revenue_rank,
  conversion_rank,
  efficiency_rank,
  -- Page health score
  CASE 
    WHEN days_since_active > 7 THEN 'INACTIVE'
    WHEN purchase_rate >= 0.05 AND revenue_per_send >= 0.10 THEN 'EXCELLENT'
    WHEN purchase_rate >= 0.03 AND revenue_per_send >= 0.05 THEN 'GOOD'
    WHEN purchase_rate >= 0.01 THEN 'FAIR'
    ELSE 'POOR'
  END as performance_tier
FROM page_ranks
ORDER BY total_revenue DESC;

-- =====================================
-- 4. AI RECOMMENDATION EFFECTIVENESS
-- =====================================

CREATE OR REPLACE VIEW `of-scheduler-proj.dashboard.v_ai_recommendation_performance` AS
WITH caption_performance AS (
  SELECT
    cr.username_page,
    cr.caption_hash,
    cr.caption_text,
    AVG(cr.p_buy_eb) as predicted_purchase_rate,
    AVG(cr.score_final) as ai_confidence_score,
    COUNT(*) as times_recommended,
    -- Actual performance from message facts
    COALESCE(SUM(CAST(mf.sent AS INT64)), 0) as actual_sent,
    COALESCE(SUM(CAST(mf.purchased AS INT64)), 0) as actual_purchased,
    COALESCE(SAFE_DIVIDE(SUM(CAST(mf.purchased AS INT64)), SUM(CAST(mf.sent AS INT64))), 0) as actual_purchase_rate,
    COALESCE(SUM(CAST(mf.earnings_usd AS FLOAT64)), 0) as actual_revenue
  FROM `of-scheduler-proj.mart.caption_rank_next24_v3_tbl` cr
  LEFT JOIN `of-scheduler-proj.core.message_facts` mf
    ON cr.username_page = mf.username_std 
    AND cr.caption_hash = mf.caption_hash
    AND DATE(cr.slot_dt_local) = DATE(mf.sending_ts)
  WHERE DATE(cr.slot_dt_local) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND cr.rn = 1  -- Only top recommendations
  GROUP BY 1, 2, 3
  HAVING actual_sent > 0  -- Only captions that were actually used
)
SELECT
  username_page,
  caption_hash,
  LEFT(caption_text, 50) as caption_preview,
  times_recommended,
  actual_sent,
  actual_purchased,
  ROUND(predicted_purchase_rate * 100, 2) as predicted_purchase_rate_pct,
  ROUND(actual_purchase_rate * 100, 2) as actual_purchase_rate_pct,
  ROUND(ai_confidence_score, 3) as ai_confidence_score,
  ROUND(actual_revenue, 2) as actual_revenue,
  -- AI accuracy metrics
  ABS(predicted_purchase_rate - actual_purchase_rate) as prediction_error,
  CASE 
    WHEN ABS(predicted_purchase_rate - actual_purchase_rate) <= 0.01 THEN 'EXCELLENT'
    WHEN ABS(predicted_purchase_rate - actual_purchase_rate) <= 0.02 THEN 'GOOD'
    WHEN ABS(predicted_purchase_rate - actual_purchase_rate) <= 0.03 THEN 'FAIR'
    ELSE 'POOR'
  END as prediction_accuracy
FROM caption_performance
ORDER BY actual_revenue DESC;

-- =====================================
-- 5. EXECUTIVE SUMMARY - KEY METRICS
-- =====================================

CREATE OR REPLACE VIEW `of-scheduler-proj.dashboard.v_executive_summary` AS
WITH current_period AS (
  SELECT
    COUNT(DISTINCT username_std) as active_pages,
    COUNT(DISTINCT DATE(sending_ts)) as active_days,
    COUNT(*) as total_messages,
    SUM(CAST(sent AS INT64)) as total_sent,
    SUM(CAST(purchased AS INT64)) as total_purchased,
    SUM(CAST(earnings_usd AS FLOAT64)) as total_revenue,
    SAFE_DIVIDE(SUM(CAST(purchased AS INT64)), SUM(CAST(sent AS INT64))) as overall_conversion_rate
  FROM `of-scheduler-proj.core.message_facts`
  WHERE DATE(sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND earnings_usd IS NOT NULL
),
previous_period AS (
  SELECT
    SUM(CAST(earnings_usd AS FLOAT64)) as prev_revenue,
    SAFE_DIVIDE(SUM(CAST(purchased AS INT64)), SUM(CAST(sent AS INT64))) as prev_conversion_rate
  FROM `of-scheduler-proj.core.message_facts`
  WHERE DATE(sending_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND DATE(sending_ts) < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND earnings_usd IS NOT NULL
)
SELECT
  -- Current metrics
  cp.active_pages,
  cp.active_days,
  cp.total_messages,
  cp.total_sent,
  cp.total_purchased,
  ROUND(cp.total_revenue, 2) as total_revenue,
  ROUND(cp.overall_conversion_rate * 100, 2) as conversion_rate_pct,
  ROUND(cp.total_revenue / cp.active_pages, 2) as revenue_per_page,
  ROUND(cp.total_revenue / cp.total_messages, 2) as revenue_per_message,
  
  -- Growth metrics
  ROUND(pp.prev_revenue, 2) as prev_period_revenue,
  ROUND(cp.total_revenue - pp.prev_revenue, 2) as revenue_change,
  ROUND(SAFE_DIVIDE(cp.total_revenue - pp.prev_revenue, pp.prev_revenue) * 100, 1) as revenue_growth_pct,
  ROUND((cp.overall_conversion_rate - pp.prev_conversion_rate) * 100, 2) as conversion_rate_change_pts,
  
  -- System health
  (SELECT COUNT(*) FROM `of-scheduler-proj.mart.caption_rank_next24_v3_tbl` WHERE DATE(slot_dt_local) = CURRENT_DATE()) as ai_predictions_today,
  (SELECT COUNT(*) FROM `of-scheduler-proj.raw.caption_library`) as total_captions_available,
  
  CURRENT_TIMESTAMP() as report_generated_at
FROM current_period cp
CROSS JOIN previous_period pp;