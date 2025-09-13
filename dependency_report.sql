+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                f0_                                                                                 |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| -- View: v_send_to_perf_link_180d                                                                                                                                  |
| WITH ss AS (                                                                                                                                                       |
|   SELECT * FROM `of-scheduler-proj.core.scheduled_send_facts`                                                                                                      |
| ),                                                                                                                                                                 |
| mm AS (                                                                                                                                                            |
|   SELECT username_std, sending_ts, caption_hash, price_usd, earnings_usd, sent, viewed, purchased, sender                                                          |
|   FROM `of-scheduler-proj.core.message_facts`                                                                                                                      |
|   WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)                                                                                         |
| ),                                                                                                                                                                 |
| cl AS (                                                                                                                                                            |
|   SELECT username_std, caption_id, caption_hash                                                                                                                    |
|   FROM `of-scheduler-proj.core.caption_dim`                                                                                                                        |
| ),                                                                                                                                                                 |
| cand AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     ss.username_std, ss.logged_ts, ss.scheduler_name, ss.caption_id, ss.was_modified,                                                                              |
|     ss.price_usd_scheduled, ss.tracking_hash,                                                                                                                      |
|     mm.sending_ts, mm.caption_hash AS hash_msg,                                                                                                                    |
|     mm.price_usd, mm.earnings_usd, mm.sent, mm.viewed, mm.purchased, mm.sender,                                                                                    |
|     CASE                                                                                                                                                           |
|       WHEN ss.tracking_hash IS NOT NULL AND ss.tracking_hash = mm.caption_hash THEN 3                                                                              |
|       WHEN ss.caption_id IS NOT NULL AND EXISTS (                                                                                                                  |
|         SELECT 1 FROM cl                                                                                                                                           |
|         WHERE cl.username_std = ss.username_std                                                                                                                    |
|           AND cl.caption_id   = ss.caption_id                                                                                                                      |
|           AND cl.caption_hash = mm.caption_hash                                                                                                                    |
|       ) THEN 2                                                                                                                                                     |
|       WHEN ss.price_usd_scheduled IS NOT NULL                                                                                                                      |
|            AND ABS(ss.price_usd_scheduled - mm.price_usd) < 0.01                                                                                                   |
|            AND mm.sending_ts BETWEEN TIMESTAMP_SUB(ss.logged_ts, INTERVAL 1 DAY)                                                                                   |
|                                  AND TIMESTAMP_ADD(ss.logged_ts, INTERVAL 14 DAY) THEN 1                                                                           |
|       ELSE 0                                                                                                                                                       |
|     END AS match_score,                                                                                                                                            |
|     ABS(TIMESTAMP_DIFF(mm.sending_ts, ss.logged_ts, MINUTE)) AS dt_min                                                                                             |
|   FROM ss                                                                                                                                                          |
|   JOIN mm USING (username_std)                                                                                                                                     |
|   WHERE (ss.tracking_hash IS NOT NULL AND ss.tracking_hash = mm.caption_hash)                                                                                      |
|      OR (ss.caption_id IS NOT NULL AND EXISTS (                                                                                                                    |
|            SELECT 1 FROM cl                                                                                                                                        |
|            WHERE cl.username_std = ss.username_std                                                                                                                 |
|              AND cl.caption_id   = ss.caption_id                                                                                                                   |
|              AND cl.caption_hash = mm.caption_hash))                                                                                                               |
|      OR (ss.price_usd_scheduled IS NOT NULL                                                                                                                        |
|          AND ABS(ss.price_usd_scheduled - mm.price_usd) < 0.01                                                                                                     |
|          AND mm.sending_ts BETWEEN TIMESTAMP_SUB(ss.logged_ts, INTERVAL 1 DAY)                                                                                     |
|                                AND TIMESTAMP_ADD(ss.logged_ts, INTERVAL 14 DAY))                                                                                   |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   username_std,                                                                                                                                                    |
|   DATE(sending_ts) AS sent_date,                                                                                                                                   |
|   scheduler_name,                                                                                                                                                  |
|   sender,                                                                                                                                                          |
|   logged_ts,                                                                                                                                                       |
|   caption_id,                                                                                                                                                      |
|   was_modified,                                                                                                                                                    |
|   price_usd_scheduled,                                                                                                                                             |
|   sending_ts,                                                                                                                                                      |
|   hash_msg,                                                                                                                                                        |
|   price_usd,                                                                                                                                                       |
|   earnings_usd,                                                                                                                                                    |
|   sent,                                                                                                                                                            |
|   viewed,                                                                                                                                                          |
|   purchased,                                                                                                                                                       |
|   CASE match_score WHEN 3 THEN 'hash' WHEN 2 THEN 'caption_id' ELSE 'time_price' END AS matched_by                                                                 |
| FROM cand                                                                                                                                                          |
| QUALIFY ROW_NUMBER() OVER (                                                                                                                                        |
|   PARTITION BY username_std, logged_ts, caption_id                                                                                                                 |
|   ORDER BY match_score DESC, dt_min ASC                                                                                                                            |
| ) = 1                                                                                                                                                              |
| -- END View: v_send_to_perf_link_180d                                                                                                                              |
| -- View: v_learning_signals_28d_v1                                                                                                                                 |
| WITH mf AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     DATE(TIMESTAMP_TRUNC(sending_ts, DAY)) AS d,                                                                                                                   |
|     SAFE_CAST(price_usd    AS NUMERIC) AS price_usd,                                                                                                               |
|     SAFE_CAST(earnings_usd AS NUMERIC) AS earnings_usd                                                                                                             |
|   FROM `of-scheduler-proj.core.message_facts`                                                                                                                      |
|   WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)                                                                                          |
| ),                                                                                                                                                                 |
| by_page AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     COUNT(*) AS sends_28d,                                                                                                                                         |
|     SUM(earnings_usd) AS earnings_28d,                                                                                                                             |
|     SAFE_DIVIDE(SUM(earnings_usd), COUNT(*)) AS rps_28d,                                                                                                           |
|     APPROX_QUANTILES(price_usd, 101)[OFFSET(50)] AS p50_price,                                                                                                     |
|     COUNTIF(earnings_usd > 0) / COUNT(*) AS sell_rate                                                                                                              |
|   FROM mf                                                                                                                                                          |
|   GROUP BY username_std                                                                                                                                            |
| ),                                                                                                                                                                 |
| trend AS (                                                                                                                                                         |
|   SELECT                                                                                                                                                           |
|     a.username_std,                                                                                                                                                |
|     SAFE_DIVIDE(a.earnings, GREATEST(a.sends,1)) AS rps_recent,                                                                                                    |
|     SAFE_DIVIDE(b.earnings, GREATEST(b.sends,1)) AS rps_prev,                                                                                                      |
|     SAFE_DIVIDE(a.earnings - b.earnings, NULLIF(b.earnings,0)) AS earnings_lift_ratio                                                                              |
|   FROM (                                                                                                                                                           |
|     SELECT username_std, COUNT(*) AS sends, SUM(earnings_usd) AS earnings                                                                                          |
|     FROM mf                                                                                                                                                        |
|     WHERE d >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)                                                                                                           |
|     GROUP BY username_std                                                                                                                                          |
|   ) a                                                                                                                                                              |
|   FULL JOIN (                                                                                                                                                      |
|     SELECT username_std, COUNT(*) AS sends, SUM(earnings_usd) AS earnings                                                                                          |
|     FROM mf                                                                                                                                                        |
|     WHERE d < DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)                                                                                                            |
|       AND d >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)                                                                                                           |
|     GROUP BY username_std                                                                                                                                          |
|   ) b USING (username_std)                                                                                                                                         |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   p.username_std, p.sends_28d, p.earnings_28d, p.rps_28d, p.p50_price, p.sell_rate,                                                                                |
|   t.rps_recent, t.rps_prev, t.earnings_lift_ratio                                                                                                                  |
| FROM by_page p                                                                                                                                                     |
| LEFT JOIN trend t USING (username_std)                                                                                                                             |
| -- END View: v_learning_signals_28d_v1                                                                                                                             |
| -- View: v_messages_local_180d                                                                                                                                     |
| SELECT                                                                                                                                                             |
|   m.*,                                                                                                                                                             |
|   DATETIME(m.sending_ts, p.tz) AS dt_local,                                                                                                                        |
|   EXTRACT(HOUR FROM DATETIME(m.sending_ts, p.tz)) AS hod_local,                                                                                                    |
|   MOD(EXTRACT(DAYOFWEEK FROM DATETIME(m.sending_ts, p.tz)) + 5, 7) AS dow_local  -- Mon=0..Sun=6                                                                   |
| FROM `of-scheduler-proj.core.message_facts` m                                                                                                                      |
| JOIN `of-scheduler-proj.core.page_dim` p USING (username_std)                                                                                                      |
| WHERE m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)                                                                                         |
| -- END View: v_messages_local_180d                                                                                                                                 |
| -- View: v_messages_active_180d                                                                                                                                    |
| SELECT m.*                                                                                                                                                         |
| FROM `of-scheduler-proj.core.message_facts` m                                                                                                                      |
| JOIN `of-scheduler-proj.core.page_dim` p USING (username_std)                                                                                                      |
| WHERE m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)                                                                                         |
| -- END View: v_messages_active_180d                                                                                                                                |
| -- View: v_weekly_template_7d_pages_final                                                                                                                          |
| SELECT *                                                                                                                                                           |
| FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`                                                                                                     |
| -- END View: v_weekly_template_7d_pages_final                                                                                                                      |
| -- View: v_weekly_feasibility_alerts                                                                                                                               |
| WITH days_to_check AS (           -- the page-days we actually plan                                                                                                |
|   SELECT DISTINCT username_std, date_local                                                                                                                         |
|   FROM `of-scheduler-proj.mart.weekly_template_7d_latest`                                                                                                          |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- clamp + swap windows once, using the same rules as the planner                                                                                                  |
| pd0 AS (                                                                                                                                                           |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN 0                                                                                                           |
|          WHEN min_hod IS NULL THEN 0                                                                                                                               |
|          ELSE GREATEST(0, LEAST(23, CAST(min_hod AS INT64))) END AS min0,                                                                                          |
|     CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN 23                                                                                                          |
|          WHEN max_hod IS NULL THEN 23                                                                                                                              |
|          ELSE GREATEST(0, LEAST(23, CAST(max_hod AS INT64))) END AS max0                                                                                           |
|   FROM `of-scheduler-proj.core.page_dim`                                                                                                                           |
|   WHERE COALESCE(LOWER(CAST(is_active AS STRING)) IN ('true','t','1','yes','y'), TRUE)                                                                             |
| ),                                                                                                                                                                 |
| pd AS (                                                                                                                                                            |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN max0 ELSE min0 END AS min_hod_eff,                                                        |
|     CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN min0 ELSE max0 END AS max_hod_eff                                                         |
|   FROM pd0                                                                                                                                                         |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- policy quota by DOW (0=Mon..6=Sun like the planner)                                                                                                             |
| policy AS (                                                                                                                                                        |
|   SELECT username_std, dow, ppv_quota                                                                                                                              |
|   FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`                                                                                                            |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| base AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     d.username_std,                                                                                                                                                |
|     d.date_local,                                                                                                                                                  |
|     p.min_hod_eff AS min_h,                                                                                                                                        |
|     p.max_hod_eff AS max_h,                                                                                                                                        |
|     q.ppv_quota   AS quota,                                                                                                                                        |
|     MOD(EXTRACT(DAYOFWEEK FROM d.date_local) + 5, 7) AS dow                                                                                                        |
|   FROM days_to_check d                                                                                                                                             |
|   JOIN pd p USING (username_std)                                                                                                                                   |
|   LEFT JOIN policy q                                                                                                                                               |
|     ON q.username_std = d.username_std                                                                                                                             |
|    AND q.dow         = MOD(EXTRACT(DAYOFWEEK FROM d.date_local) + 5, 7)                                                                                            |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| calc AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     date_local,                                                                                                                                                    |
|     dow,                                                                                                                                                           |
|     quota,                                                                                                                                                         |
|     min_h,                                                                                                                                                         |
|     max_h,                                                                                                                                                         |
|     (max_h - min_h) AS window_width,                                                                                                                               |
|     GREATEST(0, 2 * (COALESCE(quota, 0) - 1)) AS width_needed_2h                                                                                                   |
|   FROM base                                                                                                                                                        |
| )                                                                                                                                                                  |
|                                                                                                                                                                    |
| SELECT *                                                                                                                                                           |
| FROM calc                                                                                                                                                          |
| WHERE quota IS NOT NULL                                                                                                                                            |
|   AND window_width < width_needed_2h   -- impossible to satisfy ≥2h with this quota/window                                                                         |
| ORDER BY username_std, date_local                                                                                                                                  |
| -- END View: v_weekly_feasibility_alerts                                                                                                                           |
| -- View: v_mm_base_180d                                                                                                                                            |
| SELECT                                                                                                                                                             |
|   LOWER(TRIM(m.username_std)) AS username_std,                                                                                                                     |
|   m.sending_ts,                                                                                                                                                    |
|   DATETIME(m.sending_ts, p.tz) AS sending_dt_local,                                                                                                                |
|   DATE(m.sending_ts, p.tz)     AS date_local,                                                                                                                      |
|   EXTRACT(HOUR FROM DATETIME(m.sending_ts, p.tz)) AS hod_local,                                                                                                    |
|   MOD(EXTRACT(DAYOFWEEK FROM DATE(m.sending_ts, p.tz)) + 5, 7) AS dow_local,  -- Mon=0..Sun=6                                                                      |
|   SAFE_CAST(m.price_usd    AS FLOAT64) AS price_usd,                                                                                                               |
|   SAFE_CAST(m.earnings_usd AS FLOAT64) AS earnings_usd,                                                                                                            |
|   SAFE_CAST(m.sent         AS INT64)   AS sent,                                                                                                                    |
|   SAFE_CAST(m.viewed       AS INT64)   AS viewed,                                                                                                                  |
|   SAFE_CAST(m.purchased    AS INT64)   AS purchased,                                                                                                               |
|   CASE WHEN m.price_usd IS NOT NULL AND m.price_usd > 0 THEN 1 ELSE 0 END AS is_ppv                                                                                |
| FROM `of-scheduler-proj.core.message_facts` m                                                                                                                      |
| JOIN `of-scheduler-proj.core.page_dim` p USING (username_std)                                                                                                      |
| WHERE m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)                                                                                         |
| -- END View: v_mm_base_180d                                                                                                                                        |
| -- View: v_page_volume_profile_28d                                                                                                                                 |
| WITH daily AS (                                                                                                                                                    |
|   SELECT mf.username_std,                                                                                                                                          |
|          DATE(mf.sending_ts, pd.tz) AS date_local,                                                                                                                 |
|          COUNTIF(mf.price_usd > 0) AS ppv_cnt,                                                                                                                     |
|          SUM(CASE WHEN mf.price_usd > 0 THEN mf.earnings_usd ELSE 0 END) AS rev_ppv                                                                                |
|   FROM `of-scheduler-proj.core.message_facts` mf                                                                                                                   |
|   JOIN `of-scheduler-proj.core.page_dim` pd USING (username_std)                                                                                                   |
|   WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)                                                                                       |
|   GROUP BY mf.username_std, date_local                                                                                                                             |
| ),                                                                                                                                                                 |
| elastic AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     AVG(ppv_cnt) AS avg_ppv_per_day,                                                                                                                               |
|     APPROX_QUANTILES(ppv_cnt, 20)[OFFSET(18)] AS q90_ppv_per_day,                                                                                                  |
|     CORR(ppv_cnt, rev_ppv) AS corr_vol_rev,                                                                                                                        |
|     CASE WHEN VAR_POP(ppv_cnt)=0 THEN 0                                                                                                                            |
|          ELSE COVAR_POP(ppv_cnt, rev_ppv)/VAR_POP(ppv_cnt) END AS slope_rev_per_ppv                                                                                |
|   FROM daily GROUP BY username_std                                                                                                                                 |
| ),                                                                                                                                                                 |
| sends AS (                                                                                                                                                         |
|   SELECT mf.username_std,                                                                                                                                          |
|          DATE(mf.sending_ts, pd.tz) AS date_local,                                                                                                                 |
|          ROW_NUMBER() OVER (PARTITION BY mf.username_std, DATE(mf.sending_ts, pd.tz) ORDER BY mf.sending_ts) AS rn,                                                |
|          COUNT(*) OVER  (PARTITION BY mf.username_std, DATE(mf.sending_ts, pd.tz)) AS n_sends,                                                                     |
|          CASE WHEN mf.price_usd > 0 THEN mf.earnings_usd ELSE 0 END AS rev                                                                                         |
|   FROM `of-scheduler-proj.core.message_facts` mf                                                                                                                   |
|   JOIN `of-scheduler-proj.core.page_dim` pd USING (username_std)                                                                                                   |
|   WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)                                                                                       |
|     AND mf.price_usd > 0                                                                                                                                           |
| ),                                                                                                                                                                 |
| fatigue AS (                                                                                                                                                       |
|   SELECT username_std,                                                                                                                                             |
|          SAFE_DIVIDE(AVG(CASE WHEN rn=n_sends THEN rev END),                                                                                                       |
|                      NULLIF(AVG(CASE WHEN rn=1 THEN rev END),0)) AS fatigue_ratio                                                                                  |
|   FROM sends WHERE n_sends >= 2 GROUP BY username_std                                                                                                              |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   e.username_std,                                                                                                                                                  |
|   e.avg_ppv_per_day,                                                                                                                                               |
|   e.q90_ppv_per_day,                                                                                                                                               |
|   e.corr_vol_rev,                                                                                                                                                  |
|   e.slope_rev_per_ppv,                                                                                                                                             |
|   COALESCE(f.fatigue_ratio,1.0) AS fatigue_ratio,                                                                                                                  |
|   CASE                                                                                                                                                             |
|     WHEN e.slope_rev_per_ppv >= 12 AND COALESCE(f.fatigue_ratio,1.0) >= 0.70 THEN 1.50                                                                             |
|     WHEN e.slope_rev_per_ppv >=  8 AND COALESCE(f.fatigue_ratio,1.0) >= 0.65 THEN 1.30                                                                             |
|     WHEN e.slope_rev_per_ppv >=  4                                    THEN 1.15                                                                                    |
|     WHEN e.slope_rev_per_ppv <=  1 OR COALESCE(f.fatigue_ratio,1.0) < 0.50 THEN 0.90                                                                               |
|     ELSE 1.00                                                                                                                                                      |
|   END AS volume_boost                                                                                                                                              |
| FROM elastic e LEFT JOIN fatigue f USING (username_std)                                                                                                            |
| -- END View: v_page_volume_profile_28d                                                                                                                             |
| -- View: v_daily_brief_today                                                                                                                                       |
| WITH hz AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     l.username_std,                                                                                                                                                |
|     l.hod_local,                                                                                                                                                   |
|     -- time-decay weight: recent messages count more                                                                                                               |
|     SUM(earnings_usd * EXP(-TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), l.sending_ts, DAY)/60.0)) AS score                                                                 |
|   FROM `of-scheduler-proj.mart.v_messages_local_180d` l                                                                                                            |
|   GROUP BY l.username_std, l.hod_local                                                                                                                             |
| ),                                                                                                                                                                 |
| best_hours AS (                                                                                                                                                    |
|   SELECT username_std, ARRAY_AGG(hod_local ORDER BY score DESC LIMIT 5) AS best_hours_local                                                                        |
|   FROM hz                                                                                                                                                          |
|   GROUP BY username_std                                                                                                                                            |
| ),                                                                                                                                                                 |
| price_band AS (                                                                                                                                                    |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     APPROX_QUANTILES(price_usd, 20)[OFFSET(8)]  AS p25,                                                                                                            |
|     APPROX_QUANTILES(price_usd, 20)[OFFSET(10)] AS p50,                                                                                                            |
|     APPROX_QUANTILES(price_usd, 20)[OFFSET(14)] AS p75                                                                                                             |
|   FROM `of-scheduler-proj.core.message_facts`                                                                                                                      |
|   WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)                                                                                          |
|   GROUP BY username_std                                                                                                                                            |
| ),                                                                                                                                                                 |
| recent_caption_use AS (                                                                                                                                            |
|   SELECT username_std, caption_hash, MAX(DATE(sending_ts)) AS last_used_date                                                                                       |
|   FROM `of-scheduler-proj.core.message_facts`                                                                                                                      |
|   WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)                                                                                         |
|   GROUP BY username_std, caption_hash                                                                                                                              |
| ),                                                                                                                                                                 |
| caption_perf AS (                                                                                                                                                  |
|   SELECT username_std, caption_hash,                                                                                                                               |
|          SUM(earnings_usd) AS cap_rev,                                                                                                                             |
|          COUNT(*)          AS cap_msgs                                                                                                                             |
|   FROM `of-scheduler-proj.core.message_facts`                                                                                                                      |
|   WHERE caption_hash IS NOT NULL                                                                                                                                   |
|     AND sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)                                                                                         |
|   GROUP BY username_std, caption_hash                                                                                                                              |
| ),                                                                                                                                                                 |
| candidates AS (                                                                                                                                                    |
|   SELECT                                                                                                                                                           |
|     cd.username_std,                                                                                                                                               |
|     cd.caption_id,                                                                                                                                                 |
|     cd.caption_hash,                                                                                                                                               |
|     cd.caption_text,                                                                                                                                               |
|     cd.caption_type,                                                                                                                                               |
|     cd.explicitness,                                                                                                                                               |
|     cd.theme_tags,                                                                                                                                                 |
|     COALESCE(cp.cap_rev, 0) AS hist_revenue,                                                                                                                       |
|     COALESCE(rcu.last_used_date, DATE '1900-01-01') AS last_used_date                                                                                              |
|   FROM `of-scheduler-proj.core.caption_dim` cd                                                                                                                     |
|   LEFT JOIN caption_perf cp USING (username_std, caption_hash)                                                                                                     |
|   LEFT JOIN recent_caption_use rcu USING (username_std, caption_hash)                                                                                              |
| ),                                                                                                                                                                 |
| top_captions AS (                                                                                                                                                  |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     ARRAY_AGG(STRUCT(caption_id, caption_text, caption_type, explicitness, theme_tags, hist_revenue)                                                               |
|               ORDER BY (DATE_DIFF(CURRENT_DATE(), last_used_date, DAY) >= 28) DESC,                                                                                |
|                        hist_revenue DESC                                                                                                                           |
|               LIMIT 10) AS caption_suggestions                                                                                                                     |
|   FROM candidates                                                                                                                                                  |
|   GROUP BY username_std                                                                                                                                            |
| ),                                                                                                                                                                 |
| avoid_last7 AS (                                                                                                                                                   |
|   SELECT username_std, ARRAY_AGG(DISTINCT caption_hash) AS avoid_caption_hashes_7d                                                                                 |
|   FROM `of-scheduler-proj.core.message_facts`                                                                                                                      |
|   WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)                                                                                           |
|   GROUP BY username_std                                                                                                                                            |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   p.username_std,                                                                                                                                                  |
|   p.assigned_scheduler,                                                                                                                                            |
|   s.page_state,                                                                                                                                                    |
|   s.state_note,                                                                                                                                                    |
|   COALESCE(bh.best_hours_local, []) AS best_hours_local,                                                                                                           |
|   STRUCT(pb.p25, pb.p50, pb.p75)     AS price_band_suggested,                                                                                                      |
|   COALESCE(tc.caption_suggestions, []) AS caption_suggestions,                                                                                                     |
|   COALESCE(a.avoid_caption_hashes_7d, []) AS avoid_caption_hashes_7d                                                                                               |
| FROM `of-scheduler-proj.core.page_dim` p                                                                                                                           |
| LEFT JOIN best_hours  bh USING (username_std)                                                                                                                      |
| LEFT JOIN price_band  pb USING (username_std)                                                                                                                      |
| LEFT JOIN top_captions tc USING (username_std)                                                                                                                     |
| LEFT JOIN avoid_last7 a  USING (username_std)                                                                                                                      |
| LEFT JOIN `of-scheduler-proj.core.page_state` s USING (username_std)                                                                                               |
| -- END View: v_daily_brief_today                                                                                                                                   |
| -- View: v_dm_style_lift_28d_v3                                                                                                                                    |
| WITH cfg AS (                                                                                                                                                      |
|   SELECT                                                                                                                                                           |
|     CAST(COALESCE(MAX(CASE WHEN setting_key='half_life_days_rev' THEN setting_val END), '45') AS FLOAT64) AS hl_days,                                              |
|     CAST(COALESCE(MAX(CASE WHEN setting_key='prior_k_style'      THEN setting_val END), '30') AS FLOAT64) AS k_style                                               |
|   FROM `of-scheduler-proj.core.settings_modeling`                                                                                                                  |
| ),                                                                                                                                                                 |
| base AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     f.username_page, f.dow, f.hod, f.caption_hash, f.rps, f.sending_ts_utc,                                                                                        |
|     `of-scheduler-proj.util.halflife_weight`(f.sending_ts_utc, (SELECT hl_days FROM cfg)) AS w                                                                     |
|   FROM `of-scheduler-proj.mart.fn_dm_send_facts`(28) f                                                                                                             |
| ),                                                                                                                                                                 |
| slot_baseline AS (                                                                                                                                                 |
|   SELECT                                                                                                                                                           |
|     username_page, dow, hod,                                                                                                                                       |
|     SAFE_DIVIDE(SUM(rps*w), NULLIF(SUM(w),0)) AS baseline_rps,                                                                                                     |
|     SUM(w) AS slot_w                                                                                                                                               |
|   FROM base                                                                                                                                                        |
|   GROUP BY 1,2,3                                                                                                                                                   |
| ),                                                                                                                                                                 |
| feat AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     username_page, caption_hash, len_bin, emoji_bin, has_cta, has_urgency, ends_with_question                                                                      |
|   FROM `of-scheduler-proj.core.v_caption_candidates_features_v3`                                                                                                   |
| ),                                                                                                                                                                 |
| agg AS (                                                                                                                                                           |
|   SELECT                                                                                                                                                           |
|     b.username_page, b.dow, b.hod,                                                                                                                                 |
|     f.len_bin, f.emoji_bin, f.has_cta, f.has_urgency, f.ends_with_question,                                                                                        |
|     COUNT(*) AS sends,                                                                                                                                             |
|     SUM(b.w) AS eff_w,                                                                                                                                             |
|     SAFE_DIVIDE(SUM(b.rps*b.w), NULLIF(SUM(b.w),0)) AS rps_w                                                                                                       |
|   FROM base b                                                                                                                                                      |
|   JOIN feat f                                                                                                                                                      |
|     USING (username_page, caption_hash)                                                                                                                            |
|   GROUP BY 1,2,3,4,5,6,7,8                                                                                                                                         |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   a.username_page, a.dow, a.hod,                                                                                                                                   |
|   a.len_bin, a.emoji_bin, a.has_cta, a.has_urgency, a.ends_with_question,                                                                                          |
|   a.sends, a.eff_w,                                                                                                                                                |
|   a.rps_w,                                                                                                                                                         |
|   sb.baseline_rps,                                                                                                                                                 |
|   -- raw lift (decayed)                                                                                                                                            |
|   SAFE_DIVIDE(a.rps_w, NULLIF(sb.baseline_rps,0)) - 1 AS lift_vs_slot,                                                                                             |
|   -- smoothed lift: shrink towards slot baseline with k_style pseudo-weight                                                                                        |
|   SAFE_DIVIDE(                                                                                                                                                     |
|     (a.eff_w * a.rps_w + (SELECT k_style FROM cfg) * sb.baseline_rps),                                                                                             |
|     NULLIF(a.eff_w + (SELECT k_style FROM cfg), 0)                                                                                                                 |
|   ) / NULLIF(sb.baseline_rps,0) - 1 AS lift_vs_slot_smooth,                                                                                                        |
|   -- clamped (safe) lift to avoid extreme effects                                                                                                                  |
|   GREATEST(-0.50, LEAST(0.50,                                                                                                                                      |
|     SAFE_DIVIDE(                                                                                                                                                   |
|       (a.eff_w * a.rps_w + (SELECT k_style FROM cfg) * sb.baseline_rps),                                                                                           |
|       NULLIF(a.eff_w + (SELECT k_style FROM cfg), 0)                                                                                                               |
|     ) / NULLIF(sb.baseline_rps,0) - 1                                                                                                                              |
|   )) AS lift_vs_slot_smooth_clamped                                                                                                                                |
| FROM agg a                                                                                                                                                         |
| JOIN slot_baseline sb                                                                                                                                              |
|   USING (username_page, dow, hod)                                                                                                                                  |
| -- END View: v_dm_style_lift_28d_v3                                                                                                                                |
| -- View: v_scheduler_kpis_7d_28d                                                                                                                                   |
| WITH m AS (                                                                                                                                                        |
|   SELECT username_std, sending_ts, earnings_usd                                                                                                                    |
|   FROM `of-scheduler-proj.core.message_facts`                                                                                                                      |
|   WHERE sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)                                                                                          |
| ),                                                                                                                                                                 |
| assign AS (                                                                                                                                                        |
|   SELECT username_std, assigned_scheduler                                                                                                                          |
|   FROM `of-scheduler-proj.core.page_dim`                                                                                                                           |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   assign.assigned_scheduler AS scheduler,                                                                                                                          |
|   SUM(IF(m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY), m.earnings_usd, 0)) AS rev_7d,                                                        |
|   SUM(m.earnings_usd) AS rev_28d,                                                                                                                                  |
|   COUNTIF(m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS msgs_7d,                                                                          |
|   COUNT(*) AS msgs_28d                                                                                                                                             |
| FROM m                                                                                                                                                             |
| JOIN assign USING (username_std)                                                                                                                                   |
| GROUP BY scheduler                                                                                                                                                 |
| ORDER BY rev_28d DESC                                                                                                                                              |
| -- END View: v_scheduler_kpis_7d_28d                                                                                                                               |
| -- View: v_weekly_template_7d_pages                                                                                                                                |
| WITH base AS (                                                                                                                                                     |
|   SELECT * FROM `of-scheduler-proj.mart.weekly_template_7d_latest`                                                                                                 |
| ),                                                                                                                                                                 |
| types AS (                                                                                                                                                         |
|   SELECT username_std, page_type FROM `of-scheduler-proj.core.v_pages`                                                                                             |
| ),                                                                                                                                                                 |
| assign AS (                                                                                                                                                        |
|   SELECT username_std, ANY_VALUE(assigned_scheduler) AS assigned_scheduler                                                                                         |
|   FROM `of-scheduler-proj.core.page_dim`                                                                                                                           |
|   WHERE COALESCE(is_active, TRUE)                                                                                                                                  |
|   GROUP BY username_std                                                                                                                                            |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   b.username_std,                                                                                                                                                  |
|   t.page_type,                                                                                                                                                     |
|   CONCAT(b.username_std,'__',t.page_type) AS username_page,                                                                                                        |
|   COALESCE(a.assigned_scheduler, b.scheduler_name, 'unassigned') AS scheduler_name,                                                                                |
|   b.tz, b.date_local, b.slot_rank, b.hod_local, b.price_usd,                                                                                                       |
|   b.planned_local_datetime, b.scheduled_datetime_utc,                                                                                                              |
|   TO_BASE64(SHA256(CONCAT(                                                                                                                                         |
|     b.username_std,'__',t.page_type,'|',CAST(b.date_local AS STRING),'|',CAST(b.hod_local AS STRING)                                                               |
|   ))) AS tracking_hash                                                                                                                                             |
| FROM base b                                                                                                                                                        |
| JOIN types t USING (username_std)                                                                                                                                  |
| LEFT JOIN assign a USING (username_std)                                                                                                                            |
| -- END View: v_weekly_template_7d_pages                                                                                                                            |
| -- View: v_non_dm_windows_7d_v1                                                                                                                                    |
| WITH cfg AS (                                                                                                                                                      |
|   SELECT                                                                                                                                                           |
|     -- CSVs from settings                                                                                                                                          |
|     (SELECT setting_val FROM `of-scheduler-proj.core.settings_modeling`                                                                                            |
|      WHERE setting_key='drip_slots_csv'        LIMIT 1) AS drip_csv,                                                                                               |
|     (SELECT setting_val FROM `of-scheduler-proj.core.settings_modeling`                                                                                            |
|      WHERE setting_key='renewal_times_csv'     LIMIT 1) AS renew_csv,                                                                                              |
|     (SELECT setting_val FROM `of-scheduler-proj.core.settings_modeling`                                                                                            |
|      WHERE setting_key='link_drop_times_csv'   LIMIT 1) AS link_csv,                                                                                               |
|     CAST((SELECT setting_val FROM `of-scheduler-proj.core.settings_modeling`                                                                                       |
|           WHERE setting_key='randomize_offset_minutes' LIMIT 1) AS INT64) AS rand_off                                                                              |
| ),                                                                                                                                                                 |
| pages AS (                                                                                                                                                         |
|   SELECT username_std, COALESCE(tz,'UTC') AS tz                                                                                                                    |
|   FROM `of-scheduler-proj.core.page_dim`                                                                                                                           |
|   WHERE COALESCE(LOWER(CAST(is_active AS STRING)) IN ('true','t','1','yes','y'), TRUE)                                                                             |
| ),                                                                                                                                                                 |
| days AS (                                                                                                                                                          |
|   SELECT p.username_std, p.tz,                                                                                                                                     |
|          DATE_ADD(CURRENT_DATE(p.tz), INTERVAL d DAY) AS d,                                                                                                        |
|          d AS day_idx                                                                                                                                              |
|   FROM pages p                                                                                                                                                     |
|   CROSS JOIN UNNEST(GENERATE_ARRAY(0,6)) AS d                                                                                                                      |
| ),                                                                                                                                                                 |
| -- mark paid (renewals only for these)                                                                                                                             |
| paid AS (                                                                                                                                                          |
|   SELECT username_std, COALESCE(renew_on_pct,0) > 0 AS is_paid                                                                                                     |
|   FROM `of-scheduler-proj.staging.creator_stats_latest`                                                                                                            |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* -------- DripSet: "HH:MM|TYPE" where TYPE in {MM, Wall} -------- */                                                                                             |
| drip_tokens AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     dt.username_std, dt.tz, dt.d, dt.day_idx,                                                                                                                      |
|     SPLIT(tok, '|')[OFFSET(0)] AS hhmm,                                                                                                                            |
|     SPLIT(tok, '|')[OFFSET(1)] AS ch   -- 'MM' or 'Wall'                                                                                                           |
|   FROM days dt, cfg, UNNEST(SPLIT(cfg.drip_csv, ',')) AS tok                                                                                                       |
| ),                                                                                                                                                                 |
| drip AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     -- local DATETIME (no tz arg)                                                                                                                                  |
|     DATETIME(d, PARSE_TIME('%H:%M', hhmm)) AS base_dt_local,                                                                                                       |
|     ch AS channel,  -- 'MM' or 'Wall'                                                                                                                              |
|     -- deterministic jitter in [-rand_off, +rand_off]                                                                                                              |
|     CAST(                                                                                                                                                          |
|       MOD(ABS(FARM_FINGERPRINT(CONCAT(username_std,'|',CAST(d AS STRING),'|DRIP|',hhmm,'|',ch))),                                                                  |
|           2*(SELECT rand_off FROM cfg)+1                                                                                                                           |
|       ) - (SELECT rand_off FROM cfg)                                                                                                                               |
|       AS INT64                                                                                                                                                     |
|     ) AS minute_jitter,                                                                                                                                            |
|     tz                                                                                                                                                             |
|   FROM drip_tokens                                                                                                                                                 |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* -------- Renewals: paid pages only, times listed in renew_csv -------- */                                                                                       |
| renew_tokens AS (                                                                                                                                                  |
|   SELECT dt.username_std, dt.tz, dt.d, dt.day_idx, tok AS hhmm                                                                                                     |
|   FROM days dt, cfg, UNNEST(SPLIT(cfg.renew_csv, ',')) AS tok                                                                                                      |
|   JOIN paid p USING (username_std)                                                                                                                                 |
|   WHERE p.is_paid = TRUE                                                                                                                                           |
| ),                                                                                                                                                                 |
| renew AS (                                                                                                                                                         |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     DATETIME(d, PARSE_TIME('%H:%M', hhmm)) AS base_dt_local,                                                                                                       |
|     'Renewal' AS channel,                                                                                                                                          |
|     CAST(                                                                                                                                                          |
|       MOD(ABS(FARM_FINGERPRINT(CONCAT(username_std,'|',CAST(d AS STRING),'|RENEW|',hhmm))),                                                                        |
|           2*(SELECT rand_off FROM cfg)+1                                                                                                                           |
|       ) - (SELECT rand_off FROM cfg)                                                                                                                               |
|       AS INT64                                                                                                                                                     |
|     ) AS minute_jitter,                                                                                                                                            |
|     tz                                                                                                                                                             |
|   FROM renew_tokens                                                                                                                                                |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* -------- Link drops: windows like "HH:MM-HH:MM" → pick a minute inside -------- */                                                                              |
| link_tokens AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     dt.username_std, dt.tz, dt.d, dt.day_idx,                                                                                                                      |
|     SPLIT(tok, '-')[OFFSET(0)] AS hhmm_start,                                                                                                                      |
|     SPLIT(tok, '-')[OFFSET(1)] AS hhmm_end                                                                                                                         |
|   FROM days dt, cfg, UNNEST(SPLIT(cfg.link_csv, ',')) AS tok                                                                                                       |
| ),                                                                                                                                                                 |
| link_picked AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     lt.username_std,                                                                                                                                               |
|     -- local window start/end as DATETIME (no tz)                                                                                                                  |
|     DATETIME(lt.d, PARSE_TIME('%H:%M', lt.hhmm_start)) AS win_start_dt,                                                                                            |
|     DATETIME(lt.d, PARSE_TIME('%H:%M', lt.hhmm_end))   AS win_end_dt,                                                                                              |
|     'LinkDrop' AS channel,                                                                                                                                         |
|     lt.tz                                                                                                                                                          |
|   FROM link_tokens lt                                                                                                                                              |
| ),                                                                                                                                                                 |
| link_final AS (                                                                                                                                                    |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     DATETIME_ADD(win_start_dt,                                                                                                                                     |
|       INTERVAL CAST(MOD(                                                                                                                                           |
|         ABS(FARM_FINGERPRINT(CONCAT(username_std,'|',CAST(win_start_dt AS STRING),'|link'))),                                                                      |
|         GREATEST(DATETIME_DIFF(win_end_dt, win_start_dt, MINUTE), 1)                                                                                               |
|       ) AS INT64) MINUTE                                                                                                                                           |
|     ) AS base_dt_local,                                                                                                                                            |
|     'LinkDrop' AS channel,                                                                                                                                         |
|     0 AS minute_jitter,                                                                                                                                            |
|     tz                                                                                                                                                             |
|   FROM link_picked                                                                                                                                                 |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* -------- Union + compute outputs -------- */                                                                                                                    |
| unioned AS (                                                                                                                                                       |
|   SELECT * FROM drip                                                                                                                                               |
|   UNION ALL SELECT * FROM renew                                                                                                                                    |
|   UNION ALL SELECT * FROM link_final                                                                                                                               |
| ),                                                                                                                                                                 |
| with_dt AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     u.username_std,                                                                                                                                                |
|     -- final local datetime                                                                                                                                        |
|     DATETIME_ADD(u.base_dt_local, INTERVAL u.minute_jitter MINUTE) AS planned_local_datetime,                                                                      |
|     u.channel,                                                                                                                                                     |
|     u.tz                                                                                                                                                           |
|   FROM unioned u                                                                                                                                                   |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   w.username_std,                                                                                                                                                  |
|   DATE(w.planned_local_datetime) AS date_local,                                                                                                                    |
|   EXTRACT(HOUR FROM w.planned_local_datetime) AS hod_local,                                                                                                        |
|   -- map channel to a normalized kind for ops                                                                                                                      |
|   CASE                                                                                                                                                             |
|     WHEN w.channel='MM'    THEN 'drip_mm'                                                                                                                          |
|     WHEN w.channel='Wall'  THEN 'drip_wall'                                                                                                                        |
|     WHEN w.channel='Renewal' THEN 'renewal'                                                                                                                        |
|     ELSE 'link_drop'                                                                                                                                               |
|   END AS slot_kind,                                                                                                                                                |
|   w.channel,                                                                                                                                                       |
|   w.planned_local_datetime,                                                                                                                                        |
|   -- if you need UTC for automation, compute it here                                                                                                               |
|   TIMESTAMP(w.planned_local_datetime, w.tz) AS scheduled_datetime_utc                                                                                              |
| FROM with_dt w                                                                                                                                                     |
| ORDER BY username_std, planned_local_datetime, slot_kind                                                                                                           |
| -- END View: v_non_dm_windows_7d_v1                                                                                                                                |
| -- View: v_ppv_followup_bumps_v1                                                                                                                                   |
| WITH cfg AS (                                                                                                                                                      |
|   SELECT                                                                                                                                                           |
|     CAST(COALESCE(MAX(IF(setting_key = 'randomize_offset_minutes', setting_val, NULL)), '45') AS INT64) AS jitter_m                                                |
|   FROM `of-scheduler-proj.core.settings_modeling`                                                                                                                  |
| ),                                                                                                                                                                 |
| ppv AS (                                                                                                                                                           |
|   SELECT t.username_std, t.tz, t.date_local, t.slot_rank, t.hod_local, t.price_usd                                                                                 |
|   FROM `of-scheduler-proj.mart.weekly_template_7d_latest` t                                                                                                        |
|   WHERE t.price_usd > 0                                                                                                                                            |
| ),                                                                                                                                                                 |
| level AS (                                                                                                                                                         |
|   SELECT                                                                                                                                                           |
|     p.*,                                                                                                                                                           |
|     CASE                                                                                                                                                           |
|       WHEN p.price_usd >= COALESCE(pr.p90, p.price_usd) THEN 'premium'                                                                                             |
|       WHEN p.price_usd >= COALESCE(pr.p50, p.price_usd) THEN 'mid'                                                                                                 |
|       ELSE 'teaser'                                                                                                                                                |
|     END AS ppv_level                                                                                                                                               |
|   FROM ppv p                                                                                                                                                       |
|   LEFT JOIN `of-scheduler-proj.mart.v_mm_price_profile_90d_v2` pr USING (username_std)                                                                             |
| ),                                                                                                                                                                 |
| rules AS (                                                                                                                                                         |
|   -- order_i = 0 are "pre" bumps; positive order_i are follow-ups                                                                                                  |
|   SELECT 'teaser'   AS lvl, 0 AS order_i, -9999 AS min_off, -9999 AS max_off, FALSE AS is_real  -- no pre for teaser                                               |
|   UNION ALL SELECT 'teaser', 1,  20,  45, TRUE                                                                                                                     |
|   UNION ALL SELECT 'mid',    0, -20, -15, TRUE                                                                                                                     |
|   UNION ALL SELECT 'mid',    1,  15,  20, TRUE                                                                                                                     |
|   UNION ALL SELECT 'mid',    2,  45,  45, TRUE                                                                                                                     |
|   UNION ALL SELECT 'premium',0, -20, -15, TRUE                                                                                                                     |
|   UNION ALL SELECT 'premium',1,  20,  30, TRUE                                                                                                                     |
|   UNION ALL SELECT 'premium',2,  40,  55, TRUE                                                                                                                     |
| ),                                                                                                                                                                 |
| base AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     l.username_std,                                                                                                                                                |
|     l.tz,                                                                                                                                                          |
|     l.date_local,                                                                                                                                                  |
|     l.slot_rank,                                                                                                                                                   |
|     l.hod_local,                                                                                                                                                   |
|     l.ppv_level,                                                                                                                                                   |
|     r.order_i,                                                                                                                                                     |
|     r.is_real,                                                                                                                                                     |
|     DATETIME(l.date_local, TIME(l.hod_local, 0, 0)) AS base_slot_dt_local,                                                                                         |
|                                                                                                                                                                    |
|     -- Deterministic pick in [min_off, max_off] using a stable key                                                                                                 |
|     CAST(ROUND(                                                                                                                                                    |
|       r.min_off + MOD(                                                                                                                                             |
|         ABS(FARM_FINGERPRINT(CONCAT(                                                                                                                               |
|           CAST(l.username_std AS STRING),'|',                                                                                                                      |
|           CAST(l.date_local   AS STRING),'|',                                                                                                                      |
|           CAST(l.slot_rank    AS STRING),'|',                                                                                                                      |
|           CAST(r.order_i      AS STRING)                                                                                                                           |
|         ))),                                                                                                                                                       |
|         (r.max_off - r.min_off + 1)                                                                                                                                |
|       )                                                                                                                                                            |
|     ) AS INT64) AS picked_min                                                                                                                                      |
|   FROM level l                                                                                                                                                     |
|   JOIN rules r ON r.lvl = l.ppv_level                                                                                                                              |
|   WHERE r.is_real = TRUE                                                                                                                                           |
| ),                                                                                                                                                                 |
| expanded AS (                                                                                                                                                      |
|   SELECT                                                                                                                                                           |
|     b.*,                                                                                                                                                           |
|     DATETIME_ADD(b.base_slot_dt_local, INTERVAL b.picked_min MINUTE) AS planned_dt_local                                                                           |
|   FROM base b                                                                                                                                                      |
| ),                                                                                                                                                                 |
| jittered AS (                                                                                                                                                      |
|   SELECT                                                                                                                                                           |
|     e.*,                                                                                                                                                           |
|     -- Deterministic jitter in [-jitter_m, +jitter_m] using a separate key namespace ("|J")                                                                        |
|     DATETIME_ADD(                                                                                                                                                  |
|       e.planned_dt_local,                                                                                                                                          |
|       INTERVAL CAST(                                                                                                                                               |
|         MOD(                                                                                                                                                       |
|           ABS(FARM_FINGERPRINT(CONCAT(                                                                                                                             |
|             CAST(e.username_std AS STRING),'|',                                                                                                                    |
|             CAST(e.date_local   AS STRING),'|',                                                                                                                    |
|             CAST(e.slot_rank    AS STRING),'|',                                                                                                                    |
|             CAST(e.order_i      AS STRING),'|','J'                                                                                                                 |
|           ))),                                                                                                                                                     |
|           (2 * (SELECT jitter_m FROM cfg) + 1)                                                                                                                     |
|         ) - (SELECT jitter_m FROM cfg) AS INT64                                                                                                                    |
|       ) MINUTE                                                                                                                                                     |
|     ) AS planned_dt_local_j                                                                                                                                        |
|   FROM expanded e                                                                                                                                                  |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   j.username_std,                                                                                                                                                  |
|   j.date_local,                                                                                                                                                    |
|   j.slot_rank,                                                                                                                                                     |
|   j.ppv_level AS ppv_level,                                                                                                                                        |
|   CASE WHEN j.order_i = 0 AND j.ppv_level IN ('mid','premium') THEN 'ppv_pre_teaser'                                                                               |
|        ELSE 'ppv_followup' END AS activity_type,                                                                                                                   |
|   EXTRACT(HOUR   FROM j.planned_dt_local_j) AS hod_local,                                                                                                          |
|   EXTRACT(MINUTE FROM j.planned_dt_local_j) AS minute_local,                                                                                                       |
|   j.planned_dt_local_j AS planned_local_datetime,                                                                                                                  |
|   TIMESTAMP(j.planned_dt_local_j, pd.tz)    AS scheduled_datetime_utc                                                                                              |
| FROM jittered j                                                                                                                                                    |
| JOIN `of-scheduler-proj.core.page_dim` pd USING (username_std)                                                                                                     |
| -- END View: v_ppv_followup_bumps_v1                                                                                                                               |
| -- View: v_weekly_template_audit                                                                                                                                   |
| WITH q AS (                                                                                                                                                        |
|   SELECT * FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`                                                                                                   |
| ),                                                                                                                                                                 |
| w AS (                                                                                                                                                             |
|   SELECT username_std, weight_volume, weight_price, weight_hours, exploration_rate, updated_at                                                                     |
|   FROM `of-scheduler-proj.core.page_personalization_weights`                                                                                                       |
| ),                                                                                                                                                                 |
| dow AS (  -- total DOW score for context                                                                                                                           |
|   SELECT username_std, dow_local AS dow, SUM(score) AS dow_score                                                                                                   |
|   FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`                                                                                                         |
|   GROUP BY username_std, dow_local                                                                                                                                 |
| ),                                                                                                                                                                 |
| pp AS (                                                                                                                                                            |
|   SELECT username_std, p35, p50, p60, p80, p90, price_mode, corr_price_rev                                                                                         |
|   FROM `of-scheduler-proj.mart.v_mm_price_profile_90d_v2`                                                                                                          |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   t.username_std,                                                                                                                                                  |
|   t.scheduler_name,                                                                                                                                                |
|   t.date_local,                                                                                                                                                    |
|   MOD(EXTRACT(DAYOFWEEK FROM t.date_local) + 5, 7) AS dow,  -- 0=Mon..6=Sun                                                                                        |
|   q.ppv_quota,                                                                                                                                                     |
|   q.hour_pool,                                                                                                                                                     |
|   q.is_burst_dow,                                                                                                                                                  |
|   w.weight_volume, w.weight_hours, w.weight_price, w.exploration_rate,                                                                                             |
|   pp.p35, pp.p50, pp.p60, pp.p80, pp.p90, pp.price_mode, pp.corr_price_rev,                                                                                        |
|   d.dow_score,                                                                                                                                                     |
|   t.slot_rank,                                                                                                                                                     |
|   t.hod_local,                                                                                                                                                     |
|   t.price_usd,                                                                                                                                                     |
|   t.planned_local_datetime,                                                                                                                                        |
|   t.scheduled_datetime_utc                                                                                                                                         |
| FROM `of-scheduler-proj.mart.weekly_template_7d_latest` t                                                                                                          |
| LEFT JOIN q  ON q.username_std = t.username_std                                                                                                                    |
|             AND q.dow = MOD(EXTRACT(DAYOFWEEK FROM t.date_local) + 5, 7)                                                                                           |
| LEFT JOIN w  ON w.username_std = t.username_std                                                                                                                    |
| LEFT JOIN pp ON pp.username_std = t.username_std                                                                                                                   |
| LEFT JOIN dow d ON d.username_std = t.username_std                                                                                                                 |
|                AND d.dow = MOD(EXTRACT(DAYOFWEEK FROM t.date_local) + 5, 7)                                                                                        |
| ORDER BY t.username_std, t.date_local, t.slot_rank                                                                                                                 |
| -- END View: v_weekly_template_audit                                                                                                                               |
| -- View: v_slot_recommendations_next24_gated_v1                                                                                                                    |
| WITH base AS (                                                                                                                                                     |
|   SELECT username_page, slot_dt_local                                                                                                                              |
|   FROM `of-scheduler-proj.mart.v_slot_recommendations_next24_v3`                                                                                                   |
| ),                                                                                                                                                                 |
| dh AS (                                                                                                                                                            |
|   SELECT                                                                                                                                                           |
|     b.username_page,                                                                                                                                               |
|     b.slot_dt_local,                                                                                                                                               |
|     MOD(EXTRACT(DAYOFWEEK FROM b.slot_dt_local) + 5, 7) AS dow,                                                                                                    |
|     CAST(FORMAT_DATETIME('%H', b.slot_dt_local) AS INT64) AS hod                                                                                                   |
|   FROM base b                                                                                                                                                      |
| ),                                                                                                                                                                 |
| price AS (                                                                                                                                                         |
|   SELECT s.username_page, s.dow, s.hod, p.price_q AS reco_price_usd                                                                                                |
|   FROM `of-scheduler-proj.mart.v_slot_scorecard_v3` s                                                                                                              |
|   LEFT JOIN `of-scheduler-proj.mart.v_ppv_price_reco_lcb_28d_v3` p                                                                                                 |
|     ON p.username_page = s.username_page                                                                                                                           |
|    AND p.dow = s.dow                                                                                                                                               |
|    AND p.hod = s.hod                                                                                                                                               |
| ),                                                                                                                                                                 |
| paid AS (                                                                                                                                                          |
|   SELECT username_page, is_paid                                                                                                                                    |
|   FROM `of-scheduler-proj.core.v_page_paid_final_v1`                                                                                                               |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   dh.username_page,                                                                                                                                                |
|   dh.slot_dt_local,                                                                                                                                                |
|   dh.dow, dh.hod,                                                                                                                                                  |
|   CASE WHEN pr.reco_price_usd > 0 THEN 'ppv' ELSE 'free' END AS reco_dm_type,                                                                                      |
|   IFNULL(pr.reco_price_usd, 0) AS reco_price_usd                                                                                                                   |
| FROM dh                                                                                                                                                            |
| LEFT JOIN price pr USING (username_page, dow, hod)                                                                                                                 |
| LEFT JOIN paid  p  USING (username_page)                                                                                                                           |
| WHERE NOT (p.is_paid = FALSE AND pr.reco_price_usd > 0)                                                                                                            |
| -- END View: v_slot_recommendations_next24_gated_v1                                                                                                                |
| -- View: caption_ranker_vNext                                                                                                                                      |
| WITH                                                                                                                                                               |
| -- FIXED: ML weights with username_std and proper latest selection                                                                                                 |
| ml_weights AS (                                                                                                                                                    |
|   SELECT                                                                                                                                                           |
|     ps.username_std,                                                                                                                                               |
|     ps.page_state,                                                                                                                                                 |
|     w.w_rps, w.w_open, w.w_buy, w.w_dowhod, w.w_price, w.w_novelty, w.w_momentum,                                                                                  |
|     w.ucb_c, w.epsilon                                                                                                                                             |
|   FROM `of-scheduler-proj.core.page_state` ps                                                                                                                      |
|   JOIN (                                                                                                                                                           |
|     SELECT * EXCEPT(rn)                                                                                                                                            |
|     FROM (                                                                                                                                                         |
|       SELECT *,                                                                                                                                                    |
|              ROW_NUMBER() OVER (PARTITION BY page_state ORDER BY updated_at DESC) AS rn                                                                            |
|       FROM `of-scheduler-proj.ops.ml_ranking_weights_v1`                                                                                                           |
|     )                                                                                                                                                              |
|     WHERE rn = 1                                                                                                                                                   |
|   ) w USING (page_state)                                                                                                                                           |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- Get exploration config with proper latest selection                                                                                                             |
| explore_config AS (                                                                                                                                                |
|   SELECT * EXCEPT(rn)                                                                                                                                              |
|   FROM (                                                                                                                                                           |
|     SELECT *,                                                                                                                                                      |
|            ROW_NUMBER() OVER (PARTITION BY config_key ORDER BY updated_at DESC) AS rn                                                                              |
|     FROM `of-scheduler-proj.ops.explore_exploit_config_v1`                                                                                                         |
|   )                                                                                                                                                                |
|   WHERE config_key = 'default' AND rn = 1                                                                                                                          |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- FIXED: Get cooldown config from settings                                                                                                                        |
| cooldown_config AS (                                                                                                                                               |
|   SELECT                                                                                                                                                           |
|     CAST(MAX(CASE WHEN setting_key = 'min_cooldown_hours' THEN setting_value END) AS INT64) AS min_cooldown_hours,                                                 |
|     21 * 24 AS max_cooldown_hours,  -- 21 days from max_cooldown_days setting                                                                                      |
|     3 AS max_weekly_uses  -- Standard max weekly uses                                                                                                              |
|   FROM `of-scheduler-proj.core.cooldown_settings_v1`                                                                                                               |
|   WHERE setting_key IN ('min_cooldown_hours', 'max_cooldown_days')                                                                                                 |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- Get scheduled slots for next 7 days                                                                                                                             |
| scheduled_slots AS (                                                                                                                                               |
|   SELECT                                                                                                                                                           |
|     username_page,                                                                                                                                                 |
|     username_std,                                                                                                                                                  |
|     page_type,                                                                                                                                                     |
|     date_local AS slot_dt_local,                                                                                                                                   |
|     hod_local,                                                                                                                                                     |
|     slot_rank,                                                                                                                                                     |
|     tracking_hash,                                                                                                                                                 |
|     MOD(EXTRACT(DAYOFWEEK FROM date_local) + 5, 7) AS dow_local                                                                                                    |
|   FROM `of-scheduler-proj.core.v_weekly_template_7d_pages_final`                                                                                                   |
|   WHERE date_local BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)                                                                             |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- Get DOW×HOD performance patterns                                                                                                                                |
| dow_hod_patterns AS (                                                                                                                                              |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     dow_local,                                                                                                                                                     |
|     hod_local,                                                                                                                                                     |
|     score AS dow_hod_score,                                                                                                                                        |
|     PERCENT_RANK() OVER (PARTITION BY username_std ORDER BY score) AS dow_hod_percentile                                                                           |
|   FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`                                                                                                         |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- FIXED: Price elasticity - one row per page with optimal band                                                                                                    |
| price_elasticity AS (                                                                                                                                              |
|   SELECT                                                                                                                                                           |
|     username_page,                                                                                                                                                 |
|     ANY_VALUE(optimal_band) AS optimal_band,                                                                                                                       |
|     MAX_BY(band_rps, band_rps) AS optimal_band_rps                                                                                                                 |
|   FROM (                                                                                                                                                           |
|     SELECT                                                                                                                                                         |
|       username_page,                                                                                                                                               |
|       price_band,                                                                                                                                                  |
|       AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) AS band_rps,                                                                                                 |
|       FIRST_VALUE(price_band) OVER (                                                                                                                               |
|         PARTITION BY username_page                                                                                                                                 |
|         ORDER BY AVG(SAFE_DIVIDE(earnings_usd, NULLIF(sent, 0))) DESC                                                                                              |
|       ) AS optimal_band                                                                                                                                            |
|     FROM (                                                                                                                                                         |
|       SELECT                                                                                                                                                       |
|         CONCAT(mf.username_std, '__', COALESCE(pta.page_type, 'main')) AS username_page,                                                                           |
|         mf.earnings_usd,                                                                                                                                           |
|         mf.sent,                                                                                                                                                   |
|         mf.price_usd,                                                                                                                                              |
|         CASE                                                                                                                                                       |
|           WHEN mf.price_usd < 15 THEN 'low'                                                                                                                        |
|           WHEN mf.price_usd < 30 THEN 'mid'                                                                                                                        |
|           WHEN mf.price_usd < 45 THEN 'high'                                                                                                                       |
|           ELSE 'premium'                                                                                                                                           |
|         END AS price_band                                                                                                                                          |
|       FROM `of-scheduler-proj.core.message_facts` mf                                                                                                               |
|       LEFT JOIN `of-scheduler-proj.core.page_type_authority` pta                                                                                                   |
|         ON mf.username_std = pta.username_std                                                                                                                      |
|       WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)                                                                                   |
|         AND mf.sent > 0                                                                                                                                            |
|     )                                                                                                                                                              |
|     GROUP BY username_page, price_band                                                                                                                             |
|   )                                                                                                                                                                |
|   GROUP BY username_page                                                                                                                                           |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- FIXED: Cooldown check with TIMESTAMP_DIFF                                                                                                                       |
| cooldown_check AS (                                                                                                                                                |
|   SELECT                                                                                                                                                           |
|     cd.caption_id,                                                                                                                                                 |
|     CONCAT(mf.username_std, '__', COALESCE(pta.page_type, 'main')) AS username_page,                                                                               |
|     MAX(mf.sending_ts) AS last_sent_ts,                                                                                                                            |
|     COUNTIF(mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS recent_uses_7d,                                                                |
|     COUNT(DISTINCT DATE(mf.sending_ts)) AS unique_days_7d                                                                                                          |
|   FROM `of-scheduler-proj.core.message_facts` mf                                                                                                                   |
|   LEFT JOIN `of-scheduler-proj.core.page_type_authority` pta                                                                                                       |
|     ON mf.username_std = pta.username_std                                                                                                                          |
|   LEFT JOIN `of-scheduler-proj.core.caption_dim` cd                                                                                                                |
|     ON mf.caption_hash = cd.caption_hash                                                                                                                           |
|     AND mf.username_std = cd.username_std                                                                                                                          |
|   WHERE mf.caption_hash IS NOT NULL                                                                                                                                |
|     AND cd.caption_id IS NOT NULL                                                                                                                                  |
|   GROUP BY cd.caption_id, username_page                                                                                                                            |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- Calculate momentum scores                                                                                                                                       |
| momentum_scores AS (                                                                                                                                               |
|   SELECT                                                                                                                                                           |
|     CONCAT(mf.username_std, '__', COALESCE(pta.page_type, 'main')) AS username_page,                                                                               |
|     SAFE_DIVIDE(                                                                                                                                                   |
|       SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)                                                                            |
|                THEN mf.earnings_usd END),                                                                                                                          |
|       NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)                                                                     |
|                       THEN mf.sent END), 0)                                                                                                                        |
|     ) AS rps_7d,                                                                                                                                                   |
|     SAFE_DIVIDE(                                                                                                                                                   |
|       SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)                                                                           |
|                THEN mf.earnings_usd END),                                                                                                                          |
|       NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)                                                                    |
|                       THEN mf.sent END), 0)                                                                                                                        |
|     ) AS rps_30d,                                                                                                                                                  |
|     SAFE_DIVIDE(                                                                                                                                                   |
|       SAFE_DIVIDE(                                                                                                                                                 |
|         SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)                                                                          |
|                  THEN mf.earnings_usd END),                                                                                                                        |
|         NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)                                                                   |
|                         THEN mf.sent END), 0)                                                                                                                      |
|       ),                                                                                                                                                           |
|       NULLIF(SAFE_DIVIDE(                                                                                                                                          |
|         SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)                                                                         |
|                  THEN mf.earnings_usd END),                                                                                                                        |
|         NULLIF(SUM(CASE WHEN mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)                                                                  |
|                         THEN mf.sent END), 0)                                                                                                                      |
|       ), 0)                                                                                                                                                        |
|     ) AS momentum_ratio                                                                                                                                            |
|   FROM `of-scheduler-proj.core.message_facts` mf                                                                                                                   |
|   LEFT JOIN `of-scheduler-proj.core.page_type_authority` pta                                                                                                       |
|     ON mf.username_std = pta.username_std                                                                                                                          |
|   WHERE mf.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)                                                                                       |
|     AND mf.sent > 0                                                                                                                                                |
|   GROUP BY username_page                                                                                                                                           |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- FIXED: Combine features with proper JOINs (not CROSS JOIN)                                                                                                      |
| scored_captions AS (                                                                                                                                               |
|   SELECT                                                                                                                                                           |
|     ss.username_page,                                                                                                                                              |
|     ss.username_std,                                                                                                                                               |
|     ss.page_type,                                                                                                                                                  |
|     ss.slot_dt_local,                                                                                                                                              |
|     ss.hod_local,                                                                                                                                                  |
|     ss.dow_local,                                                                                                                                                  |
|     ss.slot_rank,                                                                                                                                                  |
|     ss.tracking_hash,                                                                                                                                              |
|     cf.caption_id,                                                                                                                                                 |
|     cf.caption_text,                                                                                                                                               |
|     cf.caption_hash,                                                                                                                                               |
|     cf.category,                                                                                                                                                   |
|     cf.explicitness,                                                                                                                                               |
|                                                                                                                                                                    |
|     -- Raw features                                                                                                                                                |
|     cf.conversion_rate,                                                                                                                                            |
|     cf.rps,                                                                                                                                                        |
|     cf.open_rate,                                                                                                                                                  |
|     COALESCE(dhp.dow_hod_score, 0) AS dow_hod_score,                                                                                                               |
|     COALESCE(dhp.dow_hod_percentile, 0.5) AS dow_hod_percentile,                                                                                                   |
|     cf.novelty_score,                                                                                                                                              |
|     COALESCE(ms.momentum_ratio, 1.0) AS momentum_score,                                                                                                            |
|                                                                                                                                                                    |
|     -- Normalized features                                                                                                                                         |
|     cf.rps_z_score,                                                                                                                                                |
|     cf.conversion_z_score,                                                                                                                                         |
|     cf.open_z_score,                                                                                                                                               |
|                                                                                                                                                                    |
|     -- ML weights                                                                                                                                                  |
|     mw.w_rps,                                                                                                                                                      |
|     mw.w_open,                                                                                                                                                     |
|     mw.w_buy,                                                                                                                                                      |
|     mw.w_dowhod,                                                                                                                                                   |
|     mw.w_price,                                                                                                                                                    |
|     mw.w_novelty,                                                                                                                                                  |
|     mw.w_momentum,                                                                                                                                                 |
|     mw.ucb_c,                                                                                                                                                      |
|     mw.epsilon,                                                                                                                                                    |
|                                                                                                                                                                    |
|     -- Exploration bonus                                                                                                                                           |
|     cf.exploration_bonus,                                                                                                                                          |
|     ec.max_explorer_share,                                                                                                                                         |
|                                                                                                                                                                    |
|     -- FIXED: Deterministic epsilon flag using hash                                                                                                                |
|     (ABS(FARM_FINGERPRINT(CONCAT(                                                                                                                                  |
|       cf.caption_id,                                                                                                                                               |
|       FORMAT_DATE('%Y%m%d', ss.slot_dt_local),                                                                                                                     |
|       CAST(ss.hod_local AS STRING)                                                                                                                                 |
|     ))) / 9.22e18) < mw.epsilon AS epsilon_flag,                                                                                                                   |
|                                                                                                                                                                    |
|     -- Calculate final score                                                                                                                                       |
|     (                                                                                                                                                              |
|       mw.w_rps * COALESCE(cf.rps_z_score, 0) +                                                                                                                     |
|       mw.w_open * COALESCE(cf.open_z_score, 0) +                                                                                                                   |
|       mw.w_buy * COALESCE(cf.conversion_z_score, 0) +                                                                                                              |
|       mw.w_dowhod * COALESCE((dhp.dow_hod_percentile - 0.5) * 2, 0) +                                                                                              |
|       mw.w_price * CASE                                                                                                                                            |
|         WHEN pe.optimal_band = 'mid' AND cf.rps > pe.optimal_band_rps THEN 0.2                                                                                     |
|         WHEN pe.optimal_band = 'high' AND cf.rps > pe.optimal_band_rps THEN 0.1                                                                                    |
|         ELSE 0                                                                                                                                                     |
|       END +                                                                                                                                                        |
|       mw.w_novelty * cf.novelty_score +                                                                                                                            |
|       mw.w_momentum * LEAST(1.5, GREATEST(0.5, COALESCE(ms.momentum_ratio, 1.0))) +                                                                                |
|       -- UCB exploration bonus (deterministic)                                                                                                                     |
|       CASE                                                                                                                                                         |
|         WHEN cf.is_cold_start THEN mw.ucb_c * cf.exploration_bonus                                                                                                 |
|         WHEN (ABS(FARM_FINGERPRINT(CONCAT(                                                                                                                         |
|           cf.caption_id,                                                                                                                                           |
|           FORMAT_DATE('%Y%m%d', ss.slot_dt_local),                                                                                                                 |
|           CAST(ss.hod_local AS STRING)                                                                                                                             |
|         ))) / 9.22e18) < mw.epsilon THEN 2.0                                                                                                                       |
|         ELSE 0                                                                                                                                                     |
|       END                                                                                                                                                          |
|     ) AS score_final,                                                                                                                                              |
|                                                                                                                                                                    |
|     -- FIXED: Compliance flags with config-driven thresholds                                                                                                       |
|     CASE                                                                                                                                                           |
|       WHEN cc.recent_uses_7d >= (SELECT max_weekly_uses FROM cooldown_config) THEN FALSE                                                                           |
|       WHEN cc.unique_days_7d >= 3 THEN FALSE                                                                                                                       |
|       WHEN cc.last_sent_ts IS NOT NULL                                                                                                                             |
|         AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), cc.last_sent_ts, HOUR) <                                                                                           |
|             (SELECT min_cooldown_hours FROM cooldown_config) THEN FALSE                                                                                            |
|       ELSE TRUE                                                                                                                                                    |
|     END AS cooldown_ok,                                                                                                                                            |
|                                                                                                                                                                    |
|     TRUE AS quota_ok,  -- Would join with quota table                                                                                                              |
|                                                                                                                                                                    |
|     CASE                                                                                                                                                           |
|       WHEN cc.recent_uses_7d > 0 THEN FALSE                                                                                                                        |
|       ELSE TRUE                                                                                                                                                    |
|     END AS dedupe_ok,                                                                                                                                              |
|                                                                                                                                                                    |
|     CASE                                                                                                                                                           |
|       WHEN cf.is_cold_start THEN TRUE                                                                                                                              |
|       WHEN (ABS(FARM_FINGERPRINT(CONCAT(                                                                                                                           |
|         cf.caption_id,                                                                                                                                             |
|         FORMAT_DATE('%Y%m%d', ss.slot_dt_local),                                                                                                                   |
|         CAST(ss.hod_local AS STRING)                                                                                                                               |
|       ))) / 9.22e18) < mw.epsilon THEN TRUE                                                                                                                        |
|       ELSE FALSE                                                                                                                                                   |
|     END AS is_explorer,                                                                                                                                            |
|                                                                                                                                                                    |
|     -- Metadata                                                                                                                                                    |
|     cf.total_sent,                                                                                                                                                 |
|     cf.days_since_used,                                                                                                                                            |
|     cf.is_cold_start,                                                                                                                                              |
|     cf.is_stale,                                                                                                                                                   |
|     cc.recent_uses_7d,                                                                                                                                             |
|                                                                                                                                                                    |
|     -- Reason codes                                                                                                                                                |
|     CASE                                                                                                                                                           |
|       WHEN cf.is_cold_start THEN 'cold_start_exploration'                                                                                                          |
|       WHEN (ABS(FARM_FINGERPRINT(CONCAT(                                                                                                                           |
|         cf.caption_id,                                                                                                                                             |
|         FORMAT_DATE('%Y%m%d', ss.slot_dt_local),                                                                                                                   |
|         CAST(ss.hod_local AS STRING)                                                                                                                               |
|       ))) / 9.22e18) < mw.epsilon THEN 'epsilon_exploration'                                                                                                       |
|       WHEN cf.rps_percentile > 0.8 THEN 'high_performer'                                                                                                           |
|       WHEN dhp.dow_hod_percentile > 0.7 THEN 'optimal_timing'                                                                                                      |
|       WHEN cf.novelty_score > 0.9 THEN 'fresh_content'                                                                                                             |
|       ELSE 'balanced_selection'                                                                                                                                    |
|     END AS selection_reason                                                                                                                                        |
|                                                                                                                                                                    |
|   FROM scheduled_slots ss                                                                                                                                          |
|   -- FIXED: Proper JOIN instead of CROSS JOIN                                                                                                                      |
|   INNER JOIN `of-scheduler-proj.mart.caption_features_vNext` cf                                                                                                    |
|     ON cf.username_page = ss.username_page                                                                                                                         |
|   LEFT JOIN ml_weights mw                                                                                                                                          |
|     ON ss.username_std = mw.username_std                                                                                                                           |
|   LEFT JOIN dow_hod_patterns dhp                                                                                                                                   |
|     ON ss.username_std = dhp.username_std                                                                                                                          |
|     AND ss.dow_local = dhp.dow_local                                                                                                                               |
|     AND ss.hod_local = dhp.hod_local                                                                                                                               |
|   LEFT JOIN price_elasticity pe                                                                                                                                    |
|     ON ss.username_page = pe.username_page                                                                                                                         |
|   LEFT JOIN cooldown_check cc                                                                                                                                      |
|     ON cf.caption_id = cc.caption_id                                                                                                                               |
|     AND ss.username_page = cc.username_page                                                                                                                        |
|   LEFT JOIN momentum_scores ms                                                                                                                                     |
|     ON ss.username_page = ms.username_page                                                                                                                         |
|   CROSS JOIN explore_config ec                                                                                                                                     |
|   CROSS JOIN cooldown_config                                                                                                                                       |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| -- Rank captions per slot                                                                                                                                          |
| ranked_captions AS (                                                                                                                                               |
|   SELECT                                                                                                                                                           |
|     *,                                                                                                                                                             |
|     ROW_NUMBER() OVER (                                                                                                                                            |
|       PARTITION BY username_page, slot_dt_local, hod_local                                                                                                         |
|       ORDER BY                                                                                                                                                     |
|         CASE WHEN cooldown_ok AND quota_ok AND dedupe_ok THEN 0 ELSE 1 END,                                                                                        |
|         score_final DESC,                                                                                                                                          |
|         days_since_used DESC                                                                                                                                       |
|     ) AS rank_in_slot,                                                                                                                                             |
|                                                                                                                                                                    |
|     COUNT(DISTINCT category) OVER (                                                                                                                                |
|       PARTITION BY username_page, slot_dt_local, hod_local                                                                                                         |
|     ) AS category_diversity,                                                                                                                                       |
|                                                                                                                                                                    |
|     -- FIXED: Normalize score per slot (not per page)                                                                                                              |
|     100 * (score_final - MIN(score_final) OVER (PARTITION BY username_page, slot_dt_local, hod_local)) /                                                           |
|     NULLIF(                                                                                                                                                        |
|       MAX(score_final) OVER (PARTITION BY username_page, slot_dt_local, hod_local) -                                                                               |
|       MIN(score_final) OVER (PARTITION BY username_page, slot_dt_local, hod_local),                                                                                |
|       0                                                                                                                                                            |
|     ) AS score_normalized                                                                                                                                          |
|                                                                                                                                                                    |
|   FROM scored_captions                                                                                                                                             |
|   WHERE caption_id IS NOT NULL                                                                                                                                     |
|     AND caption_text IS NOT NULL                                                                                                                                   |
| )                                                                                                                                                                  |
|                                                                                                                                                                    |
| -- Final output                                                                                                                                                    |
| SELECT                                                                                                                                                             |
|   username_page,                                                                                                                                                   |
|   username_std,                                                                                                                                                    |
|   page_type,                                                                                                                                                       |
|   slot_dt_local,                                                                                                                                                   |
|   hod_local,                                                                                                                                                       |
|   dow_local,                                                                                                                                                       |
|   slot_rank,                                                                                                                                                       |
|   tracking_hash,                                                                                                                                                   |
|   caption_id,                                                                                                                                                      |
|   caption_text,                                                                                                                                                    |
|   caption_hash,                                                                                                                                                    |
|   category,                                                                                                                                                        |
|   explicitness,                                                                                                                                                    |
|                                                                                                                                                                    |
|   ROUND(score_final, 3) AS score_final,                                                                                                                            |
|   ROUND(score_normalized, 1) AS score_normalized,                                                                                                                  |
|   rank_in_slot,                                                                                                                                                    |
|                                                                                                                                                                    |
|   ROUND(conversion_rate, 4) AS conversion_rate,                                                                                                                    |
|   ROUND(rps, 2) AS rps,                                                                                                                                            |
|   ROUND(open_rate, 4) AS open_rate,                                                                                                                                |
|                                                                                                                                                                    |
|   ROUND(dow_hod_score, 2) AS dow_hod_score,                                                                                                                        |
|   ROUND(dow_hod_percentile, 3) AS dow_hod_percentile,                                                                                                              |
|                                                                                                                                                                    |
|   ROUND(novelty_score, 3) AS novelty_score,                                                                                                                        |
|   ROUND(momentum_score, 3) AS momentum_score,                                                                                                                      |
|                                                                                                                                                                    |
|   cooldown_ok,                                                                                                                                                     |
|   quota_ok,                                                                                                                                                        |
|   dedupe_ok,                                                                                                                                                       |
|   is_explorer,                                                                                                                                                     |
|                                                                                                                                                                    |
|   total_sent,                                                                                                                                                      |
|   days_since_used,                                                                                                                                                 |
|   recent_uses_7d,                                                                                                                                                  |
|   is_cold_start,                                                                                                                                                   |
|   is_stale,                                                                                                                                                        |
|   selection_reason,                                                                                                                                                |
|   category_diversity,                                                                                                                                              |
|                                                                                                                                                                    |
|   CURRENT_TIMESTAMP() AS ranked_at,                                                                                                                                |
|   'v1.0.1-patched' AS model_version                                                                                                                                |
|                                                                                                                                                                    |
| FROM ranked_captions                                                                                                                                               |
| WHERE rank_in_slot <= 20                                                                                                                                           |
| QUALIFY ROW_NUMBER() OVER (                                                                                                                                        |
|   PARTITION BY username_page, slot_dt_local, hod_local, caption_id                                                                                                 |
|   ORDER BY rank_in_slot                                                                                                                                            |
| ) = 1                                                                                                                                                              |
| -- END View: caption_ranker_vNext                                                                                                                                  |
| -- View: v_weekly_template_7d_pages_overrides                                                                                                                      |
| WITH base AS (                                                                                                                                                     |
|   SELECT                                                                                                                                                           |
|     b.username_std, b.page_type, b.username_page, b.scheduler_name,                                                                                                |
|     b.tz, b.date_local, b.slot_rank, b.hod_local, b.price_usd,                                                                                                     |
|     b.planned_local_datetime, b.scheduled_datetime_utc, b.tracking_hash                                                                                            |
|   FROM `of-scheduler-proj.mart.v_weekly_template_7d_pages` b                                                                                                       |
| ),                                                                                                                                                                 |
| r AS (                                                                                                                                                             |
|   SELECT alias_norm, resolved_username_std                                                                                                                         |
|   FROM `of-scheduler-proj.core.v_username_resolver`                                                                                                                |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   COALESCE(r.resolved_username_std, base.username_std) AS username_std,                                                                                            |
|   base.page_type,                                                                                                                                                  |
|   CONCAT(COALESCE(r.resolved_username_std, base.username_std), '__', base.page_type) AS username_page,                                                             |
|   COALESCE(o.assigned_scheduler, base.scheduler_name, 'unassigned') AS scheduler_name,                                                                             |
|   base.tz, base.date_local, base.slot_rank, base.hod_local, base.price_usd,                                                                                        |
|   base.planned_local_datetime, base.scheduled_datetime_utc, base.tracking_hash                                                                                     |
| FROM base                                                                                                                                                          |
| LEFT JOIN r                                                                                                                                                        |
|   ON r.alias_norm = `of-scheduler-proj.util.norm_username`(base.username_std)                                                                                      |
| LEFT JOIN `of-scheduler-proj.core.page_scheduler_overrides` o                                                                                                      |
|   ON o.username_std = COALESCE(r.resolved_username_std, base.username_std)                                                                                         |
| -- END View: v_weekly_template_7d_pages_overrides                                                                                                                  |
| -- View: v_weekly_template_7d_v7                                                                                                                                   |
| WITH quota AS (                                                                                                                                                    |
|   SELECT username_std, assigned_scheduler, tz, dow, ppv_quota, hour_pool, is_burst_dow                                                                             |
|   FROM `of-scheduler-proj.mart.v_daily_quota_policy_v3`                                                                                                            |
| ),                                                                                                                                                                 |
| pd0 AS (                                                                                                                                                           |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN NULL                                                                                                        |
|          WHEN min_hod IS NULL THEN 0                                                                                                                               |
|          ELSE GREATEST(0, LEAST(23, CAST(min_hod AS INT64))) END AS min0,                                                                                          |
|     CASE WHEN min_hod IS NULL AND max_hod IS NULL THEN NULL                                                                                                        |
|          WHEN max_hod IS NULL THEN 23                                                                                                                              |
|          ELSE GREATEST(0, LEAST(23, CAST(max_hod AS INT64))) END AS max0                                                                                           |
|   FROM `of-scheduler-proj.core.page_dim`                                                                                                                           |
|   WHERE COALESCE(LOWER(CAST(is_active AS STRING)) IN ('true','t','1','yes','y'), TRUE)                                                                             |
| ),                                                                                                                                                                 |
| pd AS (                                                                                                                                                            |
|   SELECT                                                                                                                                                           |
|     username_std,                                                                                                                                                  |
|     CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN max0 ELSE min0 END AS min_hod_eff,                                                        |
|     CASE WHEN min0 IS NOT NULL AND max0 IS NOT NULL AND min0 > max0 THEN min0 ELSE max0 END AS max_hod_eff                                                         |
|   FROM pd0                                                                                                                                                         |
| ),                                                                                                                                                                 |
| weights AS (                                                                                                                                                       |
|   SELECT username_std,                                                                                                                                             |
|          COALESCE(weight_price,     1.00) AS w_price,                                                                                                              |
|          COALESCE(exploration_rate, 0.15) AS explore_rate                                                                                                          |
|   FROM `of-scheduler-proj.core.page_personalization_weights`                                                                                                       |
| ),                                                                                                                                                                 |
| state AS (                                                                                                                                                         |
|   SELECT username_std, COALESCE(page_state,'balance') AS page_state                                                                                                |
|   FROM `of-scheduler-proj.core.page_state`                                                                                                                         |
| ),                                                                                                                                                                 |
| dow_hod AS (  -- weekday×hour perf                                                                                                                                 |
|   SELECT username_std, dow_local AS dow, hod_local AS hod, score                                                                                                   |
|   FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`                                                                                                         |
| ),                                                                                                                                                                 |
| dow_pref AS (  -- pre-agg                                                                                                                                          |
|   SELECT username_std, dow, hod, SUM(score) AS s                                                                                                                   |
|   FROM dow_hod                                                                                                                                                     |
|   GROUP BY username_std, dow, hod                                                                                                                                  |
| ),                                                                                                                                                                 |
| best_global AS (  -- global fallback                                                                                                                               |
|   SELECT username_std, hod_local AS hod, SUM(score) AS s_g                                                                                                         |
|   FROM `of-scheduler-proj.mart.v_mm_dow_hod_180d_local_v2`                                                                                                         |
|   GROUP BY username_std, hod_local                                                                                                                                 |
| ),                                                                                                                                                                 |
| price_prof AS (                                                                                                                                                    |
|   SELECT username_std, p35, p50, p60, p80, p90, price_mode                                                                                                         |
|   FROM `of-scheduler-proj.mart.v_mm_price_profile_90d_v2`                                                                                                          |
| ),                                                                                                                                                                 |
| defaults AS ( SELECT ARRAY<INT64>[21,20,18,15,12,22,19,16,13,10,23,14,17,9,8,11] AS default_hours ),                                                               |
|                                                                                                                                                                    |
| /* ---------- 7 calendar days per page ---------- */                                                                                                               |
| days AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     q.username_std, q.assigned_scheduler, q.tz,                                                                                                                    |
|     p.min_hod_eff, p.max_hod_eff,                                                                                                                                  |
|     DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY) AS date_local,                                                                                                    |
|     MOD(EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY)) + 5, 7) AS dow_local,                                                                 |
|     q.ppv_quota AS quota, q.hour_pool AS hour_pool, q.is_burst_dow,                                                                                                |
|     ABS(FARM_FINGERPRINT(CONCAT(q.username_std, CAST(DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY) AS STRING)))) AS seed_day                                        |
|   FROM quota q                                                                                                                                                     |
|   JOIN pd p USING (username_std)                                                                                                                                   |
|   CROSS JOIN UNNEST(GENERATE_ARRAY(0,6)) AS d                                                                                                                      |
|   WHERE MOD(EXTRACT(DAYOFWEEK FROM DATE_ADD(CURRENT_DATE(q.tz), INTERVAL d DAY)) + 5, 7) = q.dow                                                                   |
|     AND q.ppv_quota > 0                                                                                                                                            |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* ---------- Candidate hours via JOINs ---------- */                                                                                                              |
| cand_union AS (                                                                                                                                                    |
|   -- DOW-specific                                                                                                                                                  |
|   SELECT d.*, dp.hod AS h, dp.s AS s, 1 AS src                                                                                                                     |
|   FROM days d                                                                                                                                                      |
|   JOIN dow_pref dp                                                                                                                                                 |
|     ON dp.username_std = d.username_std                                                                                                                            |
|    AND dp.dow         = d.dow_local                                                                                                                                |
|   UNION ALL                                                                                                                                                        |
|   -- global fallback                                                                                                                                               |
|   SELECT d.*, g.hod AS h, g.s_g AS s, 2 AS src                                                                                                                     |
|   FROM days d                                                                                                                                                      |
|   JOIN best_global g                                                                                                                                               |
|     ON g.username_std = d.username_std                                                                                                                             |
|   UNION ALL                                                                                                                                                        |
|   -- default last resort                                                                                                                                           |
|   SELECT d.*, h AS h, 0 AS s, 3 AS src                                                                                                                             |
|   FROM days d                                                                                                                                                      |
|   CROSS JOIN UNNEST((SELECT default_hours FROM defaults)) AS h                                                                                                     |
| ),                                                                                                                                                                 |
| cand_filtered AS (                                                                                                                                                 |
|   SELECT * FROM cand_union                                                                                                                                         |
|   WHERE h BETWEEN COALESCE(min_hod_eff,0) AND COALESCE(max_hod_eff,23)                                                                                             |
| ),                                                                                                                                                                 |
| cand_dedup AS (                                                                                                                                                    |
|   SELECT *,                                                                                                                                                        |
|          ROW_NUMBER() OVER (                                                                                                                                       |
|            PARTITION BY username_std, date_local, h                                                                                                                |
|            ORDER BY src, s DESC, h                                                                                                                                 |
|          ) AS rn_h                                                                                                                                                 |
|   FROM cand_filtered                                                                                                                                               |
| ),                                                                                                                                                                 |
| cand_ranked AS ( SELECT * FROM cand_dedup WHERE rn_h = 1 ),                                                                                                        |
| pool AS (                                                                                                                                                          |
|   SELECT                                                                                                                                                           |
|     username_std, assigned_scheduler, tz, date_local, dow_local,                                                                                                   |
|     quota, hour_pool, is_burst_dow, seed_day,                                                                                                                      |
|     COALESCE(min_hod_eff,0)  AS min_h,                                                                                                                             |
|     COALESCE(max_hod_eff,23) AS max_h,                                                                                                                             |
|     ARRAY_AGG(h ORDER BY src, s DESC, h LIMIT 24) AS hours_ranked                                                                                                  |
|   FROM cand_ranked                                                                                                                                                 |
|   GROUP BY username_std, assigned_scheduler, tz, date_local, dow_local,                                                                                            |
|            quota, hour_pool, is_burst_dow, seed_day, min_hod_eff, max_hod_eff                                                                                      |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* ---------- Segment + anchors ---------- */                                                                                                                      |
| segments AS (                                                                                                                                                      |
|   SELECT                                                                                                                                                           |
|     p.*,                                                                                                                                                           |
|     IF(ARRAY_LENGTH(p.hours_ranked) > 0, p.hours_ranked[OFFSET(0)],                               COALESCE(p.min_h, 9))  AS span_start,                            |
|     IF(ARRAY_LENGTH(p.hours_ranked) > 0, p.hours_ranked[OFFSET(ARRAY_LENGTH(p.hours_ranked)-1)], COALESCE(p.max_h, 21)) AS span_end                                |
|   FROM pool p                                                                                                                                                      |
| ),                                                                                                                                                                 |
| anchors AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     s.username_std, s.assigned_scheduler, s.tz, s.date_local, s.dow_local,                                                                                         |
|     s.quota, s.hour_pool, s.is_burst_dow, s.seed_day,                                                                                                              |
|     s.hours_ranked, s.min_h, s.max_h,                                                                                                                              |
|     LEAST(s.max_h, GREATEST(s.min_h, s.span_start)) AS a_start,                                                                                                    |
|     GREATEST(s.min_h, LEAST(s.max_h, s.span_end))   AS a_end                                                                                                       |
|   FROM segments s                                                                                                                                                  |
| ),                                                                                                                                                                 |
| anchor_grid AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     a.*,                                                                                                                                                           |
|     (a.a_end - a.a_start) AS span_len,                                                                                                                             |
|     LEAST(6, GREATEST(2,                                                                                                                                           |
|       CAST(ROUND(SAFE_DIVIDE(GREATEST(a.a_end - a.a_start, 2), GREATEST(a.quota-1, 1))) AS INT64)                                                                  |
|     )) AS seg_w                                                                                                                                                    |
|   FROM anchors a                                                                                                                                                   |
| ),                                                                                                                                                                 |
| anchor_rows AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     g.username_std, g.assigned_scheduler, g.tz, g.date_local, g.dow_local,                                                                                         |
|     g.hour_pool, g.is_burst_dow, g.seed_day, g.hours_ranked,                                                                                                       |
|     g.min_h, g.max_h, g.span_len, g.seg_w, g.quota,                                                                                                                |
|     pos AS slot_rank,                                                                                                                                              |
|     CAST(ROUND(g.a_start + pos * g.seg_w + MOD(g.seed_day + pos, 3) - 1) AS INT64) AS anchor_h,                                                                    |
|     CASE WHEN g.quota = 1 THEN CAST(ROUND((g.a_start + g.a_end)/2.0) AS INT64) ELSE NULL END AS anchor_h_center                                                    |
|   FROM anchor_grid g                                                                                                                                               |
|   CROSS JOIN UNNEST(GENERATE_ARRAY(0, LEAST(g.quota-1, 9))) AS pos                                                                                                 |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* ---------- Pick nearest candidate hour (effective pool avoids collisions) ---------- */                                                                         |
| nearest_pick AS (                                                                                                                                                  |
|   SELECT                                                                                                                                                           |
|     r.* EXCEPT(hours_ranked),                                                                                                                                      |
|     cand AS hod_cand,                                                                                                                                              |
|     off  AS cand_rank,                                                                                                                                             |
|     ROW_NUMBER() OVER (                                                                                                                                            |
|       PARTITION BY r.username_std, r.date_local, r.slot_rank                                                                                                       |
|       ORDER BY ABS(cand - COALESCE(r.anchor_h_center, r.anchor_h)), off, cand                                                                                      |
|     ) AS rn                                                                                                                                                        |
|   FROM anchor_rows r                                                                                                                                               |
|   CROSS JOIN UNNEST(r.hours_ranked) AS cand WITH OFFSET off                                                                                                        |
|   WHERE cand BETWEEN r.min_h AND r.max_h                                                                                                                           |
|     AND off < GREATEST(r.hour_pool, LEAST(ARRAY_LENGTH(r.hours_ranked), r.quota * 3))                                                                              |
| ),                                                                                                                                                                 |
| picked0 AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     username_std, assigned_scheduler, tz, date_local, dow_local,                                                                                                   |
|     slot_rank, is_burst_dow, seed_day,                                                                                                                             |
|     hod_cand AS hod_local                                                                                                                                          |
|   FROM nearest_pick                                                                                                                                                |
|   WHERE rn = 1                                                                                                                                                     |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* ---------- Closed-form spacing: enforce ≥2h and ≤6h inside [min_h, max_h] ---------- */                                                                         |
| day_bounds AS (                                                                                                                                                    |
|   SELECT username_std, date_local, MIN(min_h) AS min_h, MAX(max_h) AS max_h                                                                                        |
|   FROM pool                                                                                                                                                        |
|   GROUP BY username_std, date_local                                                                                                                                |
| ),                                                                                                                                                                 |
| ordered AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     p.*,                                                                                                                                                           |
|     ROW_NUMBER() OVER (PARTITION BY p.username_std, p.date_local ORDER BY p.hod_local) AS idx,                                                                     |
|     COUNT(*)    OVER (PARTITION BY p.username_std, p.date_local)                         AS n_slots                                                                |
|   FROM picked0 p                                                                                                                                                   |
| ),                                                                                                                                                                 |
| with_bounds AS (                                                                                                                                                   |
|   SELECT o.*, b.min_h, b.max_h                                                                                                                                     |
|   FROM ordered o                                                                                                                                                   |
|   JOIN day_bounds b USING (username_std, date_local)                                                                                                               |
| ),                                                                                                                                                                 |
| lower_env AS (  -- ensure ≥2h and start bound                                                                                                                      |
|   SELECT                                                                                                                                                           |
|     *,                                                                                                                                                             |
|     -- closed-form lower envelope: 2*idx + prefix_max(hod_local - 2*idx)                                                                                           |
|     (2*idx                                                                                                                                                         |
|       + MAX(hod_local - 2*idx) OVER (                                                                                                                              |
|           PARTITION BY username_std, date_local                                                                                                                    |
|           ORDER BY idx                                                                                                                                             |
|           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW                                                                                                         |
|         )                                                                                                                                                          |
|     )                                                          AS env2,                                                                                            |
|     -- minimal feasible hour for idx given min_h and ≥2h                                                                                                           |
|     (min_h + 2*(idx-1))                                       AS start2                                                                                            |
|   FROM with_bounds                                                                                                                                                 |
| ),                                                                                                                                                                 |
| y AS (                                                                                                                                                             |
|   SELECT                                                                                                                                                           |
|     *,                                                                                                                                                             |
|     GREATEST(hod_local, env2, start2) AS y_lower  -- apply the ≥2h lower envelope                                                                                  |
|   FROM lower_env                                                                                                                                                   |
| ),                                                                                                                                                                 |
| upper_env AS (  -- cap by ≤6h and room to finish by max_h                                                                                                          |
|   SELECT                                                                                                                                                           |
|     *,                                                                                                                                                             |
|     -- ≤6h forward cap in closed form: 6*idx + prefix_min(y_lower - 6*idx)                                                                                         |
|     (6*idx                                                                                                                                                         |
|       + MIN(y_lower - 6*idx) OVER (                                                                                                                                |
|           PARTITION BY username_std, date_local                                                                                                                    |
|           ORDER BY idx                                                                                                                                             |
|           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW                                                                                                         |
|         )                                                                                                                                                          |
|     )                                                         AS cap6,                                                                                             |
|     -- leave room for remaining slots with ≥2h up to max_h                                                                                                         |
|     (max_h - 2*(n_slots - idx))                               AS cap2_end                                                                                          |
|   FROM y                                                                                                                                                           |
| ),                                                                                                                                                                 |
| spaced AS (                                                                                                                                                        |
|   SELECT                                                                                                                                                           |
|     username_std, assigned_scheduler, tz, date_local, dow_local,                                                                                                   |
|     slot_rank, is_burst_dow, seed_day,                                                                                                                             |
|     -- final hour: inside all caps and window                                                                                                                      |
|     CAST(                                                                                                                                                          |
|       LEAST(                                                                                                                                                       |
|         GREATEST(y_lower, min_h),      -- not below lower bound/window                                                                                             |
|         cap6,                          -- ≤6h                                                                                                                      |
|         cap2_end,                      -- room to finish with ≥2h                                                                                                  |
|         max_h                          -- window top                                                                                                               |
|       ) AS INT64                                                                                                                                                   |
|     ) AS hod_final                                                                                                                                                 |
|   FROM upper_env                                                                                                                                                   |
| ),                                                                                                                                                                 |
|                                                                                                                                                                    |
| /* ---------- Price ladder ---------- */                                                                                                                           |
| ladder AS (                                                                                                                                                        |
|   SELECT                                                                                                                                                           |
|     s.username_std, s.assigned_scheduler, s.tz, s.date_local, s.dow_local,                                                                                         |
|     s.slot_rank, s.hod_final AS hod_local, s.is_burst_dow,                                                                                                         |
|     pp.p35, pp.p50, pp.p60, pp.p80, pp.p90,                                                                                                                        |
|     COALESCE(st.page_state,'balance') AS page_state,                                                                                                               |
|     COALESCE(w.w_price, 1.00) AS w_price,                                                                                                                          |
|     CASE                                                                                                                                                           |
|       WHEN COALESCE(w.w_price, 1.00) >= 1.10 THEN 'premium'                                                                                                        |
|       WHEN COALESCE(w.w_price, 1.00) <= 0.95 THEN 'value'                                                                                                          |
|       ELSE COALESCE(pp.price_mode,'balanced')                                                                                                                      |
|     END AS price_mode_eff                                                                                                                                          |
|   FROM spaced s                                                                                                                                                    |
|   LEFT JOIN price_prof pp USING (username_std)                                                                                                                     |
|   LEFT JOIN state      st USING (username_std)                                                                                                                     |
|   LEFT JOIN weights    w  USING (username_std)                                                                                                                     |
| ),                                                                                                                                                                 |
| priced_base AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     l.*,                                                                                                                                                           |
|     CAST(                                                                                                                                                          |
|       CASE                                                                                                                                                         |
|         WHEN l.price_mode_eff = 'premium' OR l.is_burst_dow = 1 THEN                                                                                               |
|           CASE l.page_state                                                                                                                                        |
|             WHEN 'grow'   THEN COALESCE(l.p60,l.p50,l.p35,6)                                                                                                       |
|             WHEN 'retain' THEN COALESCE(l.p80,l.p60,l.p50,8)                                                                                                       |
|             ELSE               COALESCE(l.p90,l.p80,l.p60,9)                                                                                                       |
|           END                                                                                                                                                      |
|         WHEN l.price_mode_eff = 'value' THEN                                                                                                                       |
|           CASE l.page_state                                                                                                                                        |
|             WHEN 'grow'   THEN COALESCE(l.p35,l.p50,5)                                                                                                             |
|             WHEN 'retain' THEN coalesce(l.p50,l.p60,6)                                                                                                             |
|             ELSE               COALESCE(l.p60,l.p50,7)                                                                                                             |
|           END                                                                                                                                                      |
|         ELSE                                                                                                                                                       |
|           CASE l.page_state                                                                                                                                        |
|             WHEN 'grow'   THEN COALESCE(l.p50,l.p35,5)                                                                                                             |
|             WHEN 'retain' THEN COALESCE(l.p60,l.p50,6)                                                                                                             |
|             ELSE               COALESCE(l.p80,l.p60,8)                                                                                                             |
|           END                                                                                                                                                      |
|       END AS FLOAT64                                                                                                                                               |
|     ) AS price1                                                                                                                                                    |
|   FROM ladder l                                                                                                                                                    |
| ),                                                                                                                                                                 |
| b1 AS ( SELECT *, price1 + (ROW_NUMBER() OVER (PARTITION BY username_std, date_local, CAST(price1 AS INT64) ORDER BY slot_rank) - 1) AS price2 FROM priced_base ), |
| b2 AS ( SELECT *, price2 + (ROW_NUMBER() OVER (PARTITION BY username_std, date_local, CAST(price2 AS INT64) ORDER BY slot_rank) - 1) AS price3 FROM b1 ),          |
| b3 AS ( SELECT *, price3 + (ROW_NUMBER() OVER (PARTITION BY username_std, date_local, CAST(price3 AS INT64) ORDER BY slot_rank) - 1) AS price4 FROM b2 )           |
| SELECT                                                                                                                                                             |
|   username_std,                                                                                                                                                    |
|   assigned_scheduler AS scheduler_name,                                                                                                                            |
|   tz,                                                                                                                                                              |
|   date_local,                                                                                                                                                      |
|   slot_rank,                                                                                                                                                       |
|   CAST(LEAST(23, GREATEST(0, hod_local)) AS INT64) AS hod_local,                                                                                                   |
|   CAST(price4 AS FLOAT64) AS price_usd,                                                                                                                            |
|   DATETIME(date_local, TIME(CAST(LEAST(23, GREATEST(0, hod_local)) AS INT64),0,0)) AS planned_local_datetime,                                                      |
|   TIMESTAMP(DATETIME(date_local, TIME(CAST(LEAST(23, GREATEST(0, hod_local)) AS INT64),0,0)), tz) AS scheduled_datetime_utc                                        |
| FROM b3                                                                                                                                                            |
| ORDER BY username_std, date_local, slot_rank                                                                                                                       |
| -- END View: v_weekly_template_7d_v7                                                                                                                               |
| -- View: v_caption_rank_next24_v3                                                                                                                                  |
| WITH params AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     CAST(COALESCE(MAX(CASE WHEN setting_key='prior_nu_buy'        THEN setting_val END), '200') AS FLOAT64) AS nu_buy,                                             |
|     CAST(COALESCE(MAX(CASE WHEN setting_key='w_style_lift'        THEN setting_val END), '0.10') AS FLOAT64) AS w_style,                                           |
|     CAST(COALESCE(MAX(CASE WHEN setting_key='ucb_sigma_min'       THEN setting_val END), '0.15') AS FLOAT64) AS ucb_sigma_min,                                     |
|     CAST(COALESCE(MAX(CASE WHEN setting_key='ucb_sigma_max'       THEN setting_val END), '0.60') AS FLOAT64) AS ucb_sigma_max,                                     |
|     CAST(COALESCE(MAX(CASE WHEN setting_key='ucb_bonus_cap_mult'  THEN setting_val END), '2.0')  AS FLOAT64) AS ucb_cap_mult                                       |
|   FROM `of-scheduler-proj.core.settings_modeling`                                                                                                                  |
| ),                                                                                                                                                                 |
| slots AS (                                                                                                                                                         |
|   SELECT username_page, slot_dt_local, dow, hod, reco_dm_type, reco_price_usd                                                                                      |
|   FROM `of-scheduler-proj.mart.v_slot_recommendations_next24_v3`                                                                                                   |
| ),                                                                                                                                                                 |
| -- candidate pool (force canonical order & types)                                                                                                                  |
| cand0_typed AS (                                                                                                                                                   |
|   SELECT                                                                                                                                                           |
|     CAST(username_page AS STRING)         AS username_page,                                                                                                        |
|     CAST(caption_id   AS STRING)          AS caption_id,                                                                                                           |
|     CAST(caption_hash AS STRING)          AS caption_hash,                                                                                                         |
|     CAST(caption_text AS STRING)          AS caption_text,                                                                                                         |
|     CAST(len_bin      AS STRING)          AS len_bin,                                                                                                              |
|     CAST(emoji_bin    AS STRING)          AS emoji_bin,                                                                                                            |
|     CAST(has_cta      AS BOOL)            AS has_cta,                                                                                                              |
|     CAST(has_urgency  AS BOOL)            AS has_urgency,                                                                                                          |
|     CAST(ends_with_question AS BOOL)      AS ends_with_question,                                                                                                   |
|     CAST(last_used_ts AS TIMESTAMP)       AS last_used_ts,                                                                                                         |
|     CAST(is_cooldown_ok AS BOOL)          AS is_cooldown_ok                                                                                                        |
|   FROM `of-scheduler-proj.mart.v_caption_candidate_pool_v3`                                                                                                        |
| ),                                                                                                                                                                 |
| pages_in_play AS (SELECT DISTINCT username_page FROM slots),                                                                                                       |
| pages_without_cand AS (                                                                                                                                            |
|   SELECT p.username_page                                                                                                                                           |
|   FROM pages_in_play p                                                                                                                                             |
|   LEFT JOIN (SELECT DISTINCT username_page FROM cand0_typed) c USING (username_page)                                                                               |
|   WHERE c.username_page IS NULL                                                                                                                                    |
| ),                                                                                                                                                                 |
| -- one synthetic (prior-only) fallback per page                                                                                                                    |
| fallback_cand_typed AS (                                                                                                                                           |
|   SELECT                                                                                                                                                           |
|     CAST(username_page AS STRING)                                    AS username_page,                                                                             |
|     CAST('fallback_default' AS STRING)                                AS caption_id,                                                                               |
|     CAST(TO_HEX(SHA256(CONCAT(username_page,'|fallback_default'))) AS STRING) AS caption_hash,                                                                     |
|     CAST(NULL AS STRING)                                             AS caption_text,                                                                              |
|     CAST('short' AS STRING)                                          AS len_bin,                                                                                   |
|     CAST('no_emoji' AS STRING)                                       AS emoji_bin,                                                                                 |
|     CAST(FALSE AS BOOL)                                              AS has_cta,                                                                                   |
|     CAST(FALSE AS BOOL)                                              AS has_urgency,                                                                               |
|     CAST(FALSE AS BOOL)                                              AS ends_with_question,                                                                        |
|     CAST(NULL  AS TIMESTAMP)                                         AS last_used_ts,                                                                              |
|     CAST(TRUE  AS BOOL)                                              AS is_cooldown_ok                                                                             |
|   FROM pages_without_cand                                                                                                                                          |
| ),                                                                                                                                                                 |
| cand AS (                                                                                                                                                          |
|   SELECT * FROM cand0_typed                                                                                                                                        |
|   UNION ALL                                                                                                                                                        |
|   SELECT * FROM fallback_cand_typed                                                                                                                                |
| ),                                                                                                                                                                 |
| style AS (SELECT * FROM `of-scheduler-proj.mart.v_dm_style_lift_28d_v3`),                                                                                          |
| stats AS (SELECT * FROM `of-scheduler-proj.mart.v_caption_decayed_stats_60d_v3`),                                                                                  |
| pri   AS (SELECT username_page, mu_buy_sent FROM `of-scheduler-proj.mart.v_page_priors_l90_v3`),                                                                   |
| -- recent volume for adaptive exploration                                                                                                                          |
| slot_vol AS (                                                                                                                                                      |
|   SELECT username_page, dow, hod, SUM(sent) AS sent_28d                                                                                                            |
|   FROM `of-scheduler-proj.mart.fn_dm_send_facts`(28)                                                                                                               |
|   GROUP BY 1,2,3                                                                                                                                                   |
| ),                                                                                                                                                                 |
| page_vol AS (                                                                                                                                                      |
|   SELECT username_page, SUM(decayed_sent) AS decayed_sent_60d                                                                                                      |
|   FROM `of-scheduler-proj.mart.v_caption_decayed_stats_60d_v3`                                                                                                     |
|   GROUP BY 1                                                                                                                                                       |
| ),                                                                                                                                                                 |
| slot_cand AS (                                                                                                                                                     |
|   SELECT                                                                                                                                                           |
|     s.username_page, s.slot_dt_local, s.dow, s.hod, s.reco_dm_type,                                                                                                |
|     GREATEST(s.reco_price_usd,0.0) AS price,                                                                                                                       |
|     c.caption_id, c.caption_hash, c.caption_text,                                                                                                                  |
|     c.len_bin, c.emoji_bin, c.has_cta, c.has_urgency, c.ends_with_question,                                                                                        |
|     c.is_cooldown_ok,                                                                                                                                              |
|     SUM(CASE WHEN c.is_cooldown_ok THEN 1 ELSE 0 END)                                                                                                              |
|       OVER (PARTITION BY s.username_page, s.slot_dt_local) AS ok_cnt_in_slot,                                                                                      |
|     COALESCE(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), c.last_used_ts, DAY), 999999) AS days_since_last_use                                                              |
|   FROM slots s                                                                                                                                                     |
|   JOIN cand  c USING (username_page)                                                                                                                               |
| ),                                                                                                                                                                 |
| styled AS (                                                                                                                                                        |
|   SELECT                                                                                                                                                           |
|     b.*,                                                                                                                                                           |
|     (SELECT w_style FROM params) * COALESCE(sl.lift_vs_slot_smooth_clamped, 0.0) AS style_score                                                                    |
|   FROM slot_cand b                                                                                                                                                 |
|   LEFT JOIN style sl                                                                                                                                               |
|     ON sl.username_page=b.username_page AND sl.dow=b.dow AND sl.hod=b.hod                                                                                          |
|    AND sl.len_bin=b.len_bin AND sl.emoji_bin=b.emoji_bin                                                                                                           |
|    AND sl.has_cta=b.has_cta AND sl.has_urgency=b.has_urgency                                                                                                       |
|    AND sl.ends_with_question=b.ends_with_question                                                                                                                  |
| ),                                                                                                                                                                 |
| eb AS (                                                                                                                                                            |
|   SELECT                                                                                                                                                           |
|     t.*,                                                                                                                                                           |
|     COALESCE(st.decayed_purchases, 0.0) AS x,                                                                                                                      |
|     COALESCE(st.decayed_sent,      0.0) AS n,                                                                                                                      |
|     COALESCE(pr.mu_buy_sent,0.08)  AS mu_page,                                                                                                                     |
|     (SELECT nu_buy FROM params)    AS nu,                                                                                                                          |
|     SAFE_DIVIDE(COALESCE(st.decayed_purchases,0.0) + (SELECT nu_buy FROM params) * COALESCE(pr.mu_buy_sent,0.08),                                                  |
|                 COALESCE(st.decayed_sent,0.0)      + (SELECT nu_buy FROM params)) AS p_buy_eb                                                                      |
|   FROM styled t                                                                                                                                                    |
|   LEFT JOIN stats st USING (username_page, caption_hash)                                                                                                           |
|   LEFT JOIN pri   pr USING (username_page)                                                                                                                         |
| ),                                                                                                                                                                 |
| ucb AS (                                                                                                                                                           |
|   SELECT                                                                                                                                                           |
|     e.*,                                                                                                                                                           |
|     GREATEST(e.n + e.nu, 1.0) AS n_eff,                                                                                                                            |
|     COALESCE(sv.sent_28d, 0.0)         AS sent_28d,                                                                                                                |
|     COALESCE(pv.decayed_sent_60d, 0.0) AS decayed_sent_60d,                                                                                                        |
|     (SELECT ucb_sigma_min FROM params) +                                                                                                                           |
|     ((SELECT ucb_sigma_max FROM params) - (SELECT ucb_sigma_min FROM params)) *                                                                                    |
|     ( 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(sv.sent_28d,0.0))))                                                                                                 |
|     + 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(pv.decayed_sent_60d,0.0)))) ) AS sigma_adapted,                                                                     |
|     CASE WHEN e.price > 0 THEN                                                                                                                                     |
|       (                                                                                                                                                            |
|         ( (SELECT ucb_sigma_min FROM params) +                                                                                                                     |
|           ((SELECT ucb_sigma_max FROM params) - (SELECT ucb_sigma_min FROM params)) *                                                                              |
|           ( 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(sv.sent_28d,0.0))))                                                                                           |
|           + 0.5 * (1.0 / (1.0 + LOG10(1.0 + COALESCE(pv.decayed_sent_60d,0.0)))) )                                                                                 |
|         )                                                                                                                                                          |
|         * e.price * SQRT( GREATEST(e.p_buy_eb*(1.0-e.p_buy_eb),0.0) / GREATEST(e.n + e.nu,1.0) )                                                                   |
|       )                                                                                                                                                            |
|     ELSE 0.0 END AS se_bonus_raw,                                                                                                                                  |
|     (e.price * e.p_buy_eb) AS rps_eb_price                                                                                                                         |
|   FROM eb e                                                                                                                                                        |
|   LEFT JOIN slot_vol sv USING (username_page, dow, hod)                                                                                                            |
|   LEFT JOIN page_vol pv USING (username_page)                                                                                                                      |
| ),                                                                                                                                                                 |
| -- allow all; tiny penalty only when cooldown had to be relaxed                                                                                                    |
| allowed AS (                                                                                                                                                       |
|   SELECT                                                                                                                                                           |
|     u.*,                                                                                                                                                           |
|     TRUE AS is_allowed,                                                                                                                                            |
|     CASE WHEN u.ok_cnt_in_slot > 0 THEN 0.0 ELSE -0.000001 * u.days_since_last_use END AS cooldown_penalty                                                         |
|   FROM ucb u                                                                                                                                                       |
| ),                                                                                                                                                                 |
| scored AS (                                                                                                                                                        |
|   SELECT                                                                                                                                                           |
|     a.*,                                                                                                                                                           |
|     LEAST(a.se_bonus_raw, (SELECT ucb_cap_mult FROM params) * a.rps_eb_price) AS se_bonus,                                                                         |
|     (a.rps_eb_price                                                                                                                                                |
|      + LEAST(a.se_bonus_raw, (SELECT ucb_cap_mult FROM params) * a.rps_eb_price)                                                                                   |
|      + COALESCE(a.style_score,0.0)                                                                                                                                 |
|      + a.cooldown_penalty) AS score_final                                                                                                                          |
|   FROM allowed a                                                                                                                                                   |
|   WHERE a.is_allowed = TRUE                                                                                                                                        |
| )                                                                                                                                                                  |
| SELECT                                                                                                                                                             |
|   username_page, slot_dt_local, dow, hod,                                                                                                                          |
|   caption_id, caption_text,                                                                                                                                        |
|   p_buy_eb, rps_eb_price, se_bonus, style_score, is_cooldown_ok,                                                                                                   |
|   score_final,                                                                                                                                                     |
|   ROW_NUMBER() OVER (PARTITION BY username_page, slot_dt_local ORDER BY score_final DESC, caption_id) AS rn                                                        |
| FROM scored                                                                                                                                                        |
| -- END View: v_caption_rank_next24_v3                                                                                                                              |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
