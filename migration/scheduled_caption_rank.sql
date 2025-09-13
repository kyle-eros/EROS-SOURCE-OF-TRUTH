-- ##############################
-- Caption Rank — Next 24h (diversity-aware staged build v2)
-- ##############################

-- ---- Parameters (settings with safe defaults)
DECLARE nu_buy        FLOAT64 DEFAULT 200.0;
DECLARE w_style       FLOAT64 DEFAULT 0.10;
DECLARE ucb_sigma_min FLOAT64 DEFAULT 0.15;
DECLARE ucb_sigma_max FLOAT64 DEFAULT 0.60;
DECLARE ucb_cap_mult  FLOAT64 DEFAULT 2.0;
DECLARE cross_block_h INT64   DEFAULT 6;
DECLARE pen_same_day  FLOAT64 DEFAULT 1.0;
DECLARE pen_cross_pg  FLOAT64 DEFAULT 0.75;
DECLARE enforce_ban   BOOL    DEFAULT FALSE;

-- ---- Settings (moved from core → ops_config)
SET nu_buy        = COALESCE((SELECT CAST(setting_val AS FLOAT64) FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='prior_nu_buy'               LIMIT 1), nu_buy);
SET w_style       = COALESCE((SELECT CAST(setting_val AS FLOAT64) FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='w_style_lift'              LIMIT 1), w_style);
SET ucb_sigma_min = COALESCE((SELECT CAST(setting_val AS FLOAT64) FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='ucb_sigma_min'             LIMIT 1), ucb_sigma_min);
SET ucb_sigma_max = COALESCE((SELECT CAST(setting_val AS FLOAT64) FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='ucb_sigma_max'             LIMIT 1), ucb_sigma_max);
SET ucb_cap_mult  = COALESCE((SELECT CAST(setting_val AS FLOAT64) FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='ucb_bonus_cap_mult'         LIMIT 1), ucb_cap_mult);
SET cross_block_h = COALESCE((SELECT CAST(setting_val AS INT64)   FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='cross_page_block_hours'    LIMIT 1), cross_block_h);
SET pen_same_day  = COALESCE((SELECT CAST(setting_val AS FLOAT64) FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='penalty_same_day_mult'     LIMIT 1), pen_same_day);
SET pen_cross_pg  = COALESCE((SELECT CAST(setting_val AS FLOAT64) FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='penalty_cross_page_mult'   LIMIT 1), pen_cross_pg);
SET enforce_ban   = COALESCE((SELECT LOWER(setting_val) IN ('true','1','yes') FROM `of-scheduler-proj.ops_config.settings_modeling` WHERE setting_key='diversity_enforce_ban' LIMIT 1), enforce_ban);

-- 0) next-24 slots
CREATE OR REPLACE TEMP TABLE tmp_slots AS
SELECT username_page, slot_dt_local, dow, hod, reco_dm_type, reco_price_usd
FROM `of-scheduler-proj.mart.v_slot_recommendations_next24_v3`;

-- 1) candidate pool (pages in play only)
CREATE OR REPLACE TEMP TABLE tmp_cand0 AS
SELECT
  CAST(c.username_page AS STRING)         AS username_page,
  CAST(c.caption_id   AS STRING)          AS caption_id,
  CAST(c.caption_hash AS STRING)          AS caption_hash,
  CAST(c.caption_text AS STRING)          AS caption_text,
  CAST(c.len_bin      AS STRING)          AS len_bin,
  CAST(c.emoji_bin    AS STRING)          AS emoji_bin,
  CAST(c.has_cta      AS BOOL)            AS has_cta,
  CAST(c.has_urgency  AS BOOL)            AS has_urgency,
  CAST(c.ends_with_question AS BOOL)      AS ends_with_question,
  CAST(c.last_used_ts AS TIMESTAMP)       AS last_used_ts,
  CAST(c.is_cooldown_ok AS BOOL)          AS is_cooldown_ok
FROM `of-scheduler-proj.mart.v_caption_candidate_pool_v3` c
JOIN (SELECT DISTINCT username_page FROM tmp_slots) p USING (username_page);

-- 2) page-fallback if page has zero candidates
CREATE OR REPLACE TEMP TABLE tmp_pages_without AS
SELECT p.username_page
FROM (SELECT DISTINCT username_page FROM tmp_slots) p
LEFT JOIN (SELECT DISTINCT username_page FROM tmp_cand0) c USING (username_page)
WHERE c.username_page IS NULL;

CREATE OR REPLACE TEMP TABLE tmp_fallback_cand AS
SELECT
  username_page,
  'fallback_default' AS caption_id,
  TO_HEX(SHA256(CONCAT(username_page,'|fallback_default'))) AS caption_hash,
  CAST(NULL AS STRING) AS caption_text,
  'short'  AS len_bin,
  'no_emoji' AS emoji_bin,
  FALSE AS has_cta, FALSE AS has_urgency, FALSE AS ends_with_question,
  CAST(NULL AS TIMESTAMP) AS last_used_ts,
  TRUE AS is_cooldown_ok
FROM tmp_pages_without;

CREATE OR REPLACE TEMP TABLE tmp_cand AS
SELECT * FROM tmp_cand0
UNION ALL
SELECT * FROM tmp_fallback_cand;

-- 3) slot × candidate + cooldown coverage
CREATE OR REPLACE TEMP TABLE tmp_slot_cand AS
SELECT
  s.username_page, s.slot_dt_local, s.dow, s.hod, s.reco_dm_type,
  GREATEST(s.reco_price_usd,0.0) AS price,
  c.caption_id, c.caption_hash, c.caption_text,
  c.len_bin, c.emoji_bin, c.has_cta, c.has_urgency, c.ends_with_question,
  c.is_cooldown_ok,
  SUM(CASE WHEN c.is_cooldown_ok THEN 1 ELSE 0 END)
    OVER (PARTITION BY s.username_page, s.slot_dt_local) AS ok_cnt_in_slot,
  COALESCE(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), c.last_used_ts, DAY), 999999) AS days_since_last_use
FROM tmp_slots s
JOIN tmp_cand  c USING (username_page);

-- 4) style score (smoothed/clamped lift)
CREATE OR REPLACE TEMP TABLE tmp_styled AS
SELECT
  b.*,
  w_style * COALESCE(sl.lift_vs_slot_smooth_clamped, 0.0) AS style_score
FROM tmp_slot_cand b
LEFT JOIN `of-scheduler-proj.mart.v_dm_style_lift_28d_v3` sl
  ON sl.username_page=b.username_page AND sl.dow=b.dow AND sl.hod=b.hod
 AND sl.len_bin=b.len_bin AND sl.emoji_bin=b.emoji_bin
 AND sl.has_cta=b.has_cta AND sl.has_urgency=b.has_urgency
 AND sl.ends_with_question=b.ends_with_question;

-- 5) EB p-buy for caption on page
CREATE OR REPLACE TEMP TABLE tmp_eb AS
SELECT
  t.*,
  COALESCE(st.decayed_purchases, 0.0) AS x,
  COALESCE(st.decayed_sent,      0.0) AS n,
  COALESCE(pr.mu_buy_sent, 0.08) AS mu_page,
  SAFE_DIVIDE(COALESCE(st.decayed_purchases,0.0) + nu_buy * COALESCE(pr.mu_buy_sent,0.08),
              COALESCE(st.decayed_sent,0.0)      + nu_buy) AS p_buy_eb
FROM tmp_styled t
LEFT JOIN `of-scheduler-proj.mart.v_caption_decayed_stats_60d_v3` st USING (username_page, caption_hash)
LEFT JOIN `of-scheduler-proj.mart.v_page_priors_l90_v3`        pr USING (username_page);

-- 6) recent volume for adaptive exploration
CREATE OR REPLACE TEMP TABLE tmp_slot_vol AS
SELECT f.username_page, f.dow, f.hod, SUM(f.sent) AS sent_28d
FROM `of-scheduler-proj.mart.fn_dm_send_facts`(28) f
JOIN (SELECT DISTINCT username_page FROM tmp_slots) p USING (username_page)
GROUP BY 1,2,3;

CREATE OR REPLACE TEMP TABLE tmp_page_vol AS
SELECT s.username_page, SUM(s.decayed_sent) AS decayed_sent_60d
FROM `of-scheduler-proj.mart.v_caption_decayed_stats_60d_v3` s
JOIN (SELECT DISTINCT username_page FROM tmp_slots) p USING (username_page)
GROUP BY 1;

-- 7) adaptive UCB (raw), prelim score (before diversity), cooldown fallback penalty
CREATE OR REPLACE TEMP TABLE tmp_prelim AS
SELECT
  e.*,
  GREATEST(e.n + nu_buy, 1.0) AS n_eff,
  COALESCE(sv.sent_28d, 0.0)         AS sent_28d,
  COALESCE(pv.decayed_sent_60d, 0.0) AS decayed_sent_60d,
  (ucb_sigma_min + (ucb_sigma_max - ucb_sigma_min) * (
     0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(sv.sent_28d,0.0)))) +
     0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(pv.decayed_sent_60d,0.0))))
   )) AS sigma_adapted,
  (e.price * e.p_buy_eb) AS rps_eb_price,
  CASE WHEN e.price > 0 THEN
    (ucb_sigma_min + (ucb_sigma_max - ucb_sigma_min) * (
       0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(sv.sent_28d,0.0)))) +
       0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(pv.decayed_sent_60d,0.0))))
     )) * e.price * SQRT( GREATEST(e.p_buy_eb*(1.0-e.p_buy_eb), 0.0) / GREATEST(e.n + nu_buy, 1.0) )
  ELSE 0.0 END AS se_bonus_raw,
  CASE WHEN e.is_cooldown_ok OR e.ok_cnt_in_slot > 0 THEN 0.0 ELSE -0.000001 * e.days_since_last_use END AS cooldown_penalty,
  ( (e.price * e.p_buy_eb)
    + LEAST( CASE WHEN e.price>0 THEN (ucb_sigma_min + (ucb_sigma_max - ucb_sigma_min) * (
              0.5*(1.0/(1.0+LOG10(1.0+COALESCE(sv.sent_28d,0.0)))) +
              0.5*(1.0/(1.0+LOG10(1.0+COALESCE(pv.decayed_sent_60d,0.0))))
            )) * e.price * SQRT( GREATEST(e.p_buy_eb*(1.0-e.p_buy_eb),0.0) / GREATEST(e.n + nu_buy,1.0) )
            ELSE 0.0 END,
            ucb_cap_mult * (e.price * e.p_buy_eb) )
    + COALESCE(e.style_score,0.0)
    + CASE WHEN e.is_cooldown_ok OR e.ok_cnt_in_slot > 0 THEN 0.0 ELSE -0.000001 * e.days_since_last_use END
  ) AS prelim_score
FROM tmp_eb e
LEFT JOIN tmp_slot_vol sv USING (username_page, dow, hod)
LEFT JOIN tmp_page_vol pv USING (username_page);

-- 8a) per-page, same-day rank of a caption (1 = best; duplicates >1)
CREATE OR REPLACE TEMP TABLE tmp_same_day AS
SELECT
  username_page,
  DATE(slot_dt_local) AS d,
  caption_hash,
  ROW_NUMBER() OVER (
    PARTITION BY username_page, DATE(slot_dt_local), caption_hash
    ORDER BY prelim_score DESC, caption_id
  ) AS same_day_rank
FROM tmp_prelim;

-- 8b) provisional top-1 per slot
CREATE OR REPLACE TEMP TABLE tmp_top1 AS
SELECT username_page, slot_dt_local, caption_hash
FROM (
  SELECT
    username_page, slot_dt_local, caption_hash,
    ROW_NUMBER() OVER (
      PARTITION BY username_page, slot_dt_local
      ORDER BY prelim_score DESC, caption_id
    ) AS rn
  FROM tmp_prelim
)
WHERE rn = 1;

-- 8c) cross-page conflict: same caption is top-1 elsewhere within ±H hours
CREATE OR REPLACE TEMP TABLE tmp_conflict AS
SELECT DISTINCT
  p.username_page,
  p.slot_dt_local,
  p.caption_hash,
  1 AS has_conflict
FROM tmp_prelim p
JOIN tmp_top1    t
  ON t.caption_hash = p.caption_hash
 AND t.username_page <> p.username_page
 AND ABS(TIMESTAMP_DIFF(t.slot_dt_local, p.slot_dt_local, HOUR)) <= cross_block_h;

-- 9) final score with penalties / bans and write FINAL table
CREATE OR REPLACE TABLE `of-scheduler-proj.mart.caption_rank_next24_v3_tbl`
PARTITION BY DATE(slot_dt_local)
CLUSTER BY username_page, dow, hod AS
WITH flags AS (
  SELECT
    p.*,
    sd.same_day_rank,
    COALESCE(cf.has_conflict, 0) AS cross_conflict
  FROM tmp_prelim p
  LEFT JOIN `tmp_same_day` sd
    ON sd.username_page=p.username_page
   AND sd.d=DATE(p.slot_dt_local)
   AND sd.caption_hash=p.caption_hash
  LEFT JOIN `tmp_conflict` cf
    ON cf.username_page=p.username_page
   AND cf.slot_dt_local=p.slot_dt_local
   AND cf.caption_hash=p.caption_hash
),
scored AS (
  SELECT
    f.*,
    LEAST(f.se_bonus_raw, ucb_cap_mult * f.rps_eb_price) AS se_bonus,
    -- penalty mode: subtract scaled penalties; ban mode: filtered in WHERE
    f.prelim_score
      - CASE WHEN NOT enforce_ban AND f.same_day_rank>1
             THEN pen_same_day * GREATEST(f.rps_eb_price, 0.0005) ELSE 0 END
      - CASE WHEN NOT enforce_ban AND f.cross_conflict=1
             THEN pen_cross_pg * GREATEST(f.rps_eb_price, 0.0005) ELSE 0 END
      AS score_final
  FROM flags f
)
SELECT
  username_page,
  slot_dt_local,
  dow,
  hod,
  caption_id,
  caption_hash,                 -- useful for joins/debug
  caption_text,
  p_buy_eb,
  rps_eb_price,
  se_bonus,
  style_score,
  is_cooldown_ok,
  score_final,
  ROW_NUMBER() OVER (
    PARTITION BY username_page, slot_dt_local
    ORDER BY score_final DESC, caption_id
  ) AS rn
FROM scored
WHERE
  (NOT enforce_ban OR same_day_rank = 1)
  AND (NOT enforce_ban OR cross_conflict = 0);
