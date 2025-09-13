WITH cfg AS (
  SELECT
    CAST(COALESCE(MAX(CASE WHEN setting_key='half_life_days_rev' THEN setting_val END), '45') AS FLOAT64) AS hl_days,
    CAST(COALESCE(MAX(CASE WHEN setting_key='prior_k_style'      THEN setting_val END), '30') AS FLOAT64) AS k_style
  FROM `of-scheduler-proj.ops_config.settings_modeling`
),
base AS (
  SELECT
    f.username_page, f.dow, f.hod, f.caption_hash, f.rps, f.sending_ts_utc,
    `of-scheduler-proj.util.halflife_weight`(f.sending_ts_utc, (SELECT hl_days FROM cfg)) AS w
  FROM `of-scheduler-proj.mart.fn_dm_send_facts`(28) f
),
slot_baseline AS (
  SELECT
    username_page, dow, hod,
    SAFE_DIVIDE(SUM(rps*w), NULLIF(SUM(w),0)) AS baseline_rps,
    SUM(w) AS slot_w
  FROM base
  GROUP BY 1,2,3
),
feat AS (
  SELECT
    username_page, caption_hash, len_bin, emoji_bin, has_cta, has_urgency, ends_with_question
  FROM `of-scheduler-proj.layer_05_ml.v_caption_features`
),
agg AS (
  SELECT
    b.username_page, b.dow, b.hod,
    f.len_bin, f.emoji_bin, f.has_cta, f.has_urgency, f.ends_with_question,
    COUNT(*) AS sends,
    SUM(b.w) AS eff_w,
    SAFE_DIVIDE(SUM(b.rps*b.w), NULLIF(SUM(b.w),0)) AS rps_w
  FROM base b
  JOIN feat f
    USING (username_page, caption_hash)
  GROUP BY 1,2,3,4,5,6,7,8
)
SELECT
  a.username_page, a.dow, a.hod,
  a.len_bin, a.emoji_bin, a.has_cta, a.has_urgency, a.ends_with_question,
  a.sends, a.eff_w,
  a.rps_w,
  sb.baseline_rps,
  -- raw lift (decayed)
  SAFE_DIVIDE(a.rps_w, NULLIF(sb.baseline_rps,0)) - 1 AS lift_vs_slot,
  -- smoothed lift: shrink towards slot baseline with k_style pseudo-weight
  SAFE_DIVIDE(
    (a.eff_w * a.rps_w + (SELECT k_style FROM cfg) * sb.baseline_rps),
    NULLIF(a.eff_w + (SELECT k_style FROM cfg), 0)
  ) / NULLIF(sb.baseline_rps,0) - 1 AS lift_vs_slot_smooth,
  -- clamped (safe) lift to avoid extreme effects
  GREATEST(-0.50, LEAST(0.50,
    SAFE_DIVIDE(
      (a.eff_w * a.rps_w + (SELECT k_style FROM cfg) * sb.baseline_rps),
      NULLIF(a.eff_w + (SELECT k_style FROM cfg), 0)
    ) / NULLIF(sb.baseline_rps,0) - 1
  )) AS lift_vs_slot_smooth_clamped
FROM agg a
JOIN slot_baseline sb
  USING (username_page, dow, hod)
