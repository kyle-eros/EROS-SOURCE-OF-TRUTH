WITH quota AS (
  SELECT username_std, assigned_scheduler, tz, dow, ppv_quota, hour_pool, is_burst_dow
  FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`
),
pd0 AS (
  SELECT
    username_std,
    CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN NULL
         WHEN min_hod IS NULL THEN 0
         ELSE GREATEST(0, LEAST(23, CAST(min_hod AS INT64))) END AS min0,
    CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN NULL
         WHEN max_hod IS NULL THEN 23
         ELSE GREATEST(0, LEAST(23, CAST(max_hod AS INT64))) END AS max0
  FROM `of-scheduler-proj.layer_04_semantic.v_page_dim`
  WHERE COALESCE(LOWER(CAST(is_active AS STRING)) IN ('true','t','1','yes','y'), TRUE)
),
pd AS (
  SELECT
    username_std,
    CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN max0 ELSE min0 END AS min_hod_eff,
    CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN min0 ELSE max0 END AS max_hod_eff
  FROM pd0
),
weights AS (
  SELECT username_std,
         COALESCE(weight_price,     1.00) AS w_price,
         COALESCE(exploration_rate, 0.15) AS explore_rate
  FROM `of-scheduler-proj.ops_config.page_personalization_weights`
),
state AS (
  SELECT username_std, COALESCE(page_state,'balance') AS page_state
  FROM `of-scheduler-proj.ops_config.page_state`
),
dow_hod AS (  -- weekday×hour perf
  SELECT username_std, dow_local AS dow, hod_local AS hod, score
  FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`
),
dow_pref AS (  -- pre-agg
  SELECT username_std, dow, hod, SUM(score) AS s
  FROM dow_hod
  GROUP BY username_std, dow, hod
),
best_global AS (  -- global fallback
  SELECT username_std, hod_local AS hod, SUM(score) AS s_g
  FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`
  GROUP BY username_std, hod_local
),
price_prof AS (
  SELECT username_std, p35, p50, p60, p80, p90, price_mode
  FROM `of-scheduler-proj.mart.v_mm_price_profile_90d_v2`
),
defaults AS ( SELECT ARRAY<INT64>[21,20,18,15,12,22,19,16,13,10,23,14,17,9,8,11] AS default_hours ),

/* ---------- 7 calendar days per page ---------- */
days AS (
  SELECT
    q.username_std, q.assigned_scheduler, q.tz,
    p.min_hod_eff, p.max_hod_eff,
    DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY) AS date_local,
    MOD(EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY)) + 5, 7) AS dow_local,
    q.ppv_quota AS quota, q.hour_pool AS hour_pool, q.is_burst_dow,
    ABS(FARM_FINGERPRINT(CONCAT(q.username_std, CAST(DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY) AS STRING)))) AS seed_day
  FROM quota q
  JOIN pd p USING (username_std)
  CROSS JOIN UNNEST(GENERATE_ARRAY(0,6)) AS d
  WHERE MOD(EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY)) + 5, 7) = q.dow
    AND q.ppv_quota > 0
),

/* ---------- Candidate hours via JOINs ---------- */
cand_union AS (
  -- DOW-specific
  SELECT d.*, dp.hod AS h, dp.s AS s, 1 AS src
  FROM days d
  JOIN dow_pref dp
    ON dp.username_std = d.username_std
   AND dp.dow         = d.dow_local
  UNION ALL
  -- global fallback
  SELECT d.*, g.hod AS h, g.s_g AS s, 2 AS src
  FROM days d
  JOIN best_global g
    ON g.username_std = d.username_std
  UNION ALL
  -- default last resort
  SELECT d.*, h AS h, 0 AS s, 3 AS src
  FROM days d
  CROSS JOIN UNNEST((SELECT default_hours FROM defaults)) AS h
),
cand_filtered AS (
  SELECT * FROM cand_union
  WHERE h BETWEEN COALESCE(min_hod_eff,0) AND COALESCE(max_hod_eff,23)
),
cand_dedup AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY username_std, date_local, h
           ORDER BY src, s DESC, h
         ) AS rn_h
  FROM cand_filtered
),
cand_ranked AS ( SELECT * FROM cand_dedup WHERE rn_h = 1 ),
pool AS (
  SELECT
    username_std, assigned_scheduler, tz, date_local, dow_local,
    quota, hour_pool, is_burst_dow, seed_day,
    COALESCE(min_hod_eff,0)  AS min_h,
    COALESCE(max_hod_eff,23) AS max_h,
    ARRAY_AGG(h ORDER BY src, s DESC, h LIMIT 24) AS hours_ranked
  FROM cand_ranked
  GROUP BY username_std, assigned_scheduler, tz, date_local, dow_local,
           quota, hour_pool, is_burst_dow, seed_day, min_hod_eff, max_hod_eff
),

/* ---------- Segment + anchors ---------- */
segments AS (
  SELECT
    p.*,
    IF(ARRAY_LENGTH(p.hours_ranked) > 0, p.hours_ranked[OFFSET(0)],                               COALESCE(p.min_h, 9))  AS span_start,
    IF(ARRAY_LENGTH(p.hours_ranked) > 0, p.hours_ranked[OFFSET(ARRAY_LENGTH(p.hours_ranked)-1)], COALESCE(p.max_h, 21)) AS span_end
  FROM pool p
),
anchors AS (
  SELECT
    s.username_std, s.assigned_scheduler, s.tz, s.date_local, s.dow_local,
    s.quota, s.hour_pool, s.is_burst_dow, s.seed_day,
    s.hours_ranked, s.min_h, s.max_h,
    LEAST(s.max_h, GREATEST(s.min_h, s.span_start)) AS a_start,
    GREATEST(s.min_h, LEAST(s.max_h, s.span_end))   AS a_end
  FROM segments s
),
anchor_grid AS (
  SELECT
    a.*,
    (a.a_end - a.a_start) AS span_len,
    LEAST(6, GREATEST(2,
      CAST(ROUND(SAFE_DIVIDE(GREATEST(a.a_end - a.a_start, 2), GREATEST(a.quota-1, 1))) AS INT64)
    )) AS seg_w
  FROM anchors a
),
anchor_rows AS (
  SELECT
    g.username_std, g.assigned_scheduler, g.tz, g.date_local, g.dow_local,
    g.hour_pool, g.is_burst_dow, g.seed_day, g.hours_ranked,
    g.min_h, g.max_h, g.span_len, g.seg_w, g.quota,
    pos AS slot_rank,
    CAST(ROUND(g.a_start + pos * g.seg_w + MOD(g.seed_day + pos, 3) - 1) AS INT64) AS anchor_h,
    CASE WHEN g.quota = 1 THEN CAST(ROUND((g.a_start + g.a_end)/2.0) AS INT64) ELSE NULL END AS anchor_h_center
  FROM anchor_grid g
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, LEAST(g.quota-1, 9))) AS pos
),

/* ---------- Pick nearest candidate hour (effective pool avoids collisions) ---------- */
nearest_pick AS (
  SELECT
    r.* EXCEPT(hours_ranked),
    cand AS hod_cand,
    off  AS cand_rank,
    ROW_NUMBER() OVER (
      PARTITION BY r.username_std, r.date_local, r.slot_rank
      ORDER BY ABS(cand - COALESCE(r.anchor_h_center, r.anchor_h)), off, cand
    ) AS rn
  FROM anchor_rows r
  CROSS JOIN UNNEST(r.hours_ranked) AS cand WITH OFFSET off
  WHERE cand BETWEEN r.min_h AND r.max_h
    AND off < GREATEST(r.hour_pool, LEAST(ARRAY_LENGTH(r.hours_ranked), r.quota * 3))
),
picked0 AS (
  SELECT
    username_std, assigned_scheduler, tz, date_local, dow_local,
    slot_rank, is_burst_dow, seed_day,
    hod_cand AS hod_local
  FROM nearest_pick
  WHERE rn = 1
),

/* ---------- Closed-form spacing: enforce ≥2h and ≤6h inside [min_h, max_h] ---------- */
day_bounds AS (
  SELECT username_std, date_local, MIN(min_h) AS min_h, MAX(max_h) AS max_h
  FROM pool
  GROUP BY username_std, date_local
),
ordered AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (PARTITION BY p.username_std, p.date_local ORDER BY p.hod_local) AS idx,
    COUNT(*)    OVER (PARTITION BY p.username_std, p.date_local)                         AS n_slots
  FROM picked0 p
),
with_bounds AS (
  SELECT o.*, b.min_h, b.max_h
  FROM ordered o
  JOIN day_bounds b USING (username_std, date_local)
),
lower_env AS (  -- ensure ≥2h and start bound
  SELECT
    *,
    -- closed-form lower envelope: 2*idx + prefix_max(hod_local - 2*idx)
    (2*idx
      + MAX(hod_local - 2*idx) OVER (
          PARTITION BY username_std, date_local
          ORDER BY idx
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    )                                                          AS env2,
    -- minimal feasible hour for idx given min_h and ≥2h
    (min_h + 2*(idx-1))                                       AS start2
  FROM with_bounds
),
y AS (
  SELECT
    *,
    GREATEST(hod_local, env2, start2) AS y_lower  -- apply the ≥2h lower envelope
  FROM lower_env
),
upper_env AS (  -- cap by ≤6h and room to finish by max_h
  SELECT
    *,
    -- ≤6h forward cap in closed form: 6*idx + prefix_min(y_lower - 6*idx)
    (6*idx
      + MIN(y_lower - 6*idx) OVER (
          PARTITION BY username_std, date_local
          ORDER BY idx
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    )                                                         AS cap6,
    -- leave room for remaining slots with ≥2h up to max_h
    (max_h - 2*(n_slots - idx))                               AS cap2_end
  FROM y
),
spaced AS (
  SELECT
    username_std, assigned_scheduler, tz, date_local, dow_local,
    slot_rank, is_burst_dow, seed_day,
    -- final hour: inside all caps and window
    CAST(
      LEAST(
        GREATEST(y_lower, min_h),      -- not below lower bound/window
        cap6,                          -- ≤6h
        cap2_end,                      -- room to finish with ≥2h
        max_h                          -- window top
      ) AS INT64
    ) AS hod_final
  FROM upper_env
),

/* ---------- Price ladder ---------- */
ladder AS (
  SELECT
    s.username_std, s.assigned_scheduler, s.tz, s.date_local, s.dow_local,
    s.slot_rank, s.hod_final AS hod_local, s.is_burst_dow,
    pp.p35, pp.p50, pp.p60, pp.p80, pp.p90,
    COALESCE(st.page_state,'balance') AS page_state,
    COALESCE(w.w_price, 1.00) AS w_price,
    CASE
      WHEN COALESCE(w.w_price, 1.00) >= 1.10 THEN 'premium'
      WHEN COALESCE(w.w_price, 1.00) <= 0.95 THEN 'value'
      ELSE COALESCE(pp.price_mode,'balanced')
    END AS price_mode_eff
  FROM spaced s
  LEFT JOIN price_prof pp USING (username_std)
  LEFT JOIN state      st USING (username_std)
  LEFT JOIN weights    w  USING (username_std)
),
priced_base AS (
  SELECT
    l.*,
    CAST(
      CASE
        WHEN l.price_mode_eff = 'premium' OR l.is_burst_dow = 1 THEN
          CASE l.page_state
            WHEN 'grow'   THEN COALESCE(l.p60,l.p50,l.p35,6)
            WHEN 'retain' THEN COALESCE(l.p80,l.p60,l.p50,8)
            ELSE               COALESCE(l.p90,l.p80,l.p60,9)
          END
        WHEN l.price_mode_eff = 'value' THEN
          CASE l.page_state
            WHEN 'grow'   THEN COALESCE(l.p35,l.p50,5)
            WHEN 'retain' THEN coalesce(l.p50,l.p60,6)
            ELSE               COALESCE(l.p60,l.p50,7)
          END
        ELSE
          CASE l.page_state
            WHEN 'grow'   THEN COALESCE(l.p50,l.p35,5)
            WHEN 'retain' THEN COALESCE(l.p60,l.p50,6)
            ELSE               COALESCE(l.p80,l.p60,8)
          END
      END AS FLOAT64
    ) AS price1
  FROM ladder l
),
b1 AS ( SELECT *, price1 + (ROW_NUMBER() OVER (PARTITION BY username_std, date_local, CAST(price1 AS INT64) ORDER BY slot_rank) - 1) AS price2 FROM priced_base ),
b2 AS ( SELECT *, price2 + (ROW_NUMBER() OVER (PARTITION BY username_std, date_local, CAST(price2 AS INT64) ORDER BY slot_rank) - 1) AS price3 FROM b1 ),
b3 AS ( SELECT *, price3 + (ROW_NUMBER() OVER (PARTITION BY username_std, date_local, CAST(price3 AS INT64) ORDER BY slot_rank) - 1) AS price4 FROM b2 )
SELECT
  username_std,
  assigned_scheduler AS scheduler_name,
  tz,
  date_local,
  slot_rank,
  CAST(LEAST(23, GREATEST(0, hod_local)) AS INT64) AS hod_local,
  CAST(price4 AS FLOAT64) AS price_usd,
  DATETIME(date_local, TIME(CAST(LEAST(23, GREATEST(0, hod_local)) AS INT64),0,0)) AS planned_local_datetime,
  TIMESTAMP(DATETIME(date_local, TIME(CAST(LEAST(23, GREATEST(0, hod_local)) AS INT64),0,0)), tz) AS scheduled_datetime_utc
FROM b3
ORDER BY username_std, date_local, slot_rank
