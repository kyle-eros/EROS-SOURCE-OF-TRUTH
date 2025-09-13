SELECT
  m.*,
  DATETIME(m.sending_ts, p.tz) AS dt_local,
  EXTRACT(HOUR FROM DATETIME(m.sending_ts, p.tz)) AS hod_local,
  MOD(EXTRACT(DAYOFWEEK FROM DATETIME(m.sending_ts, p.tz)) + 5, 7) AS dow_local  -- Mon=0..Sun=6
FROM `of-scheduler-proj.layer_04_semantic.message_facts` m
JOIN `of-scheduler-proj.layer_04_semantic.v_page_dim` p USING (username_std)
WHERE m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
