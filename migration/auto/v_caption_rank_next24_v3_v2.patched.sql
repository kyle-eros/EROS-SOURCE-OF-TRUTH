WITH params AS (
  SELECT
    CAST(COALESCE(MAX(CASE WHEN setting_key='prior_nu_buy'        THEN setting_val END), '200') AS FLOAT64) AS nu_buy,
    CAST(COALESCE(MAX(CASE WHEN setting_key='w_style_lift'        THEN setting_val END), '0.10') AS FLOAT64) AS w_style,
    CAST(COALESCE(MAX(CASE WHEN setting_key='ucb_sigma_min'       THEN setting_val END), '0.15') AS FLOAT64) AS ucb_sigma_min,
    CAST(COALESCE(MAX(CASE WHEN setting_key='ucb_sigma_max'       THEN setting_val END), '0.60') AS FLOAT64) AS ucb_sigma_max,
    CAST(COALESCE(MAX(CASE WHEN setting_key='ucb_bonus_cap_mult'  THEN setting_val END), '2.0')  AS FLOAT64) AS ucb_cap_mult
  FROM `of-scheduler-proj.ops_config.settings_modeling`
),
slots AS (
  SELECT username_page, slot_dt_local, dow, hod, reco_dm_type, reco_price_usd
  FROM `of-scheduler-proj.mart.v_slot_recommendations_next24_v3`
),
-- candidate pool (force canonical order & types)
cand0_typed AS (
  SELECT
    CAST(username_page AS STRING)         AS username_page,
    CAST(caption_id   AS STRING)          AS caption_id,
    CAST(caption_hash AS STRING)          AS caption_hash,
    CAST(caption_text AS STRING)          AS caption_text,
    CAST(len_bin      AS STRING)          AS len_bin,
    CAST(emoji_bin    AS STRING)          AS emoji_bin,
    CAST(has_cta      AS BOOL)            AS has_cta,
    CAST(has_urgency  AS BOOL)            AS has_urgency,
    CAST(ends_with_question AS BOOL)      AS ends_with_question,
    CAST(last_used_ts AS TIMESTAMP)       AS last_used_ts,
    CAST(is_cooldown_ok AS BOOL)          AS is_cooldown_ok
  FROM `of-scheduler-proj.mart.v_caption_candidate_pool_v3`
),
pages_in_play AS (SELECT DISTINCT username_page FROM slots),
pages_without_cand AS (
  SELECT p.username_page
  FROM pages_in_play p
  LEFT JOIN (SELECT DISTINCT username_page FROM cand0_typed) c USING (username_page)
  WHERE c.username_page IS NULL
),
-- one synthetic (prior-only) fallback per page
fallback_cand_typed AS (
  SELECT
    CAST(username_page AS STRING)                                    AS username_page,
    CAST('fallback_default' AS STRING)                                AS caption_id,
    CAST(TO_HEX(SHA256(CONCAT(username_page,'|fallback_default'))) AS STRING) AS caption_hash,
    CAST(NULL AS STRING)                                             AS caption_text,
    CAST('short' AS STRING)                                          AS len_bin,
    CAST('no_emoji' AS STRING)                                       AS emoji_bin,
    CAST(FALSE AS BOOL)                                              AS has_cta,
    CAST(FALSE AS BOOL)                                              AS has_urgency,
    CAST(FALSE AS BOOL)                                              AS ends_with_question,
    CAST(NULL  AS TIMESTAMP)                                         AS last_used_ts,
    CAST(TRUE  AS BOOL)                                              AS is_cooldown_ok
  FROM pages_without_cand
),
cand AS (
  SELECT * FROM cand0_typed
  UNION ALL
  SELECT * FROM fallback_cand_typed
),
style AS (SELECT * FROM `of-scheduler-proj.mart.v_dm_style_lift_28d_v3`),
stats AS (SELECT * FROM `of-scheduler-proj.mart.v_caption_decayed_stats_60d_v3`),
pri   AS (SELECT username_page, mu_buy_sent FROM `of-scheduler-proj.mart.v_page_priors_l90_v3`),
-- recent volume for adaptive exploration
slot_vol AS (
  SELECT username_page, dow, hod, SUM(sent) AS sent_28d
  FROM `of-scheduler-proj.mart.fn_dm_send_facts`(28)
  GROUP BY 1,2,3
),
page_vol AS (
  SELECT username_page, SUM(decayed_sent) AS decayed_sent_60d
  FROM `of-scheduler-proj.mart.v_caption_decayed_stats_60d_v3`
  GROUP BY 1
),
slot_cand AS (
  SELECT
    s.username_page, s.slot_dt_local, s.dow, s.hod, s.reco_dm_type,
    GREATEST(s.reco_price_usd,0.0) AS price,
    c.caption_id, c.caption_hash, c.caption_text,
    c.len_bin, c.emoji_bin, c.has_cta, c.has_urgency, c.ends_with_question,
    c.is_cooldown_ok,
    SUM(CASE WHEN c.is_cooldown_ok THEN 1 ELSE 0 END)
      OVER (PARTITION BY s.username_page, s.slot_dt_local) AS ok_cnt_in_slot,
    COALESCE(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), c.last_used_ts, DAY), 999999) AS days_since_last_use
  FROM slots s
  JOIN cand  c USING (username_page)
),
styled AS (
  SELECT
    b.*,
    (SELECT w_style FROM params) * COALESCE(sl.lift_vs_slot_smooth_clamped, 0.0) AS style_score
  FROM slot_cand b
  LEFT JOIN style sl
    ON sl.username_page=b.username_page AND sl.dow=b.dow AND sl.hod=b.hod
   AND sl.len_bin=b.len_bin AND sl.emoji_bin=b.emoji_bin
   AND sl.has_cta=b.has_cta AND sl.has_urgency=b.has_urgency
   AND sl.ends_with_question=b.ends_with_question
),
eb AS (
  SELECT
    t.*,
    COALESCE(st.decayed_purchases, 0.0) AS x,
    COALESCE(st.decayed_sent,      0.0) AS n,
    COALESCE(pr.mu_buy_sent,0.08)  AS mu_page,
    (SELECT nu_buy FROM params)    AS nu,
    SAFE_DIVIDE(COALESCE(st.decayed_purchases,0.0) + (SELECT nu_buy FROM params) * COALESCE(pr.mu_buy_sent,0.08),
                COALESCE(st.decayed_sent,0.0)      + (SELECT nu_buy FROM params)) AS p_buy_eb
  FROM styled t
  LEFT JOIN stats st USING (username_page, caption_hash)
  LEFT JOIN pri   pr USING (username_page)
),
ucb AS (
  SELECT
    e.*,
    GREATEST(e.n + e.nu, 1.0) AS n_eff,
    COALESCE(sv.sent_28d, 0.0)         AS sent_28d,
    COALESCE(pv.decayed_sent_60d, 0.0) AS decayed_sent_60d,
    (SELECT ucb_sigma_min FROM params) +
    ((SELECT ucb_sigma_max FROM params) - (SELECT ucb_sigma_min FROM params)) *
    ( 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(sv.sent_28d,0.0))))
    + 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(pv.decayed_sent_60d,0.0)))) ) AS sigma_adapted,
    CASE WHEN e.price > 0 THEN
      (
        ( (SELECT ucb_sigma_min FROM params) +
          ((SELECT ucb_sigma_max FROM params) - (SELECT ucb_sigma_min FROM params)) *
          ( 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(sv.sent_28d,0.0))))
          + 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(pv.decayed_sent_60d,0.0)))) )
        )
        * e.price * SQRT( GREATEST(e.p_buy_eb*(1.0-e.p_buy_eb),0.0) / GREATEST(e.n + e.nu,1.0) )
      )
    ELSE 0.0 END AS se_bonus_raw,
    (e.price * e.p_buy_eb) AS rps_eb_price
  FROM eb e
  LEFT JOIN slot_vol sv USING (username_page, dow, hod)
  LEFT JOIN page_vol pv USING (username_page)
),
-- allow all; tiny penalty only when cooldown had to be relaxed
allowed AS (
  SELECT
    u.*,
    TRUE AS is_allowed,
    CASE WHEN u.ok_cnt_in_slot > 0 THEN 0.0 ELSE -0.000001 * u.days_since_last_use END AS cooldown_penalty
  FROM ucb u
),
scored AS (
  SELECT
    a.*,
    LEAST(a.se_bonus_raw, (SELECT ucb_cap_mult FROM params) * a.rps_eb_price) AS se_bonus,
    (a.rps_eb_price
     + LEAST(a.se_bonus_raw, (SELECT ucb_cap_mult FROM params) * a.rps_eb_price)
     + COALESCE(a.style_score,0.0)
     + a.cooldown_penalty) AS score_final
  FROM allowed a
  WHERE a.is_allowed = TRUE
)
SELECT
  username_page, slot_dt_local, dow, hod,
  caption_id, caption_text,
  p_buy_eb, rps_eb_price, se_bonus, style_score, is_cooldown_ok,
  score_final,
  ROW_NUMBER() OVER (PARTITION BY username_page, slot_dt_local ORDER BY score_final DESC, caption_id) AS rn
FROM scored
