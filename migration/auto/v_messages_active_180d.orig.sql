SELECT m.*
FROM `of-scheduler-proj.core.message_facts` m
JOIN `of-scheduler-proj.core.page_dim` p USING (username_std)
WHERE m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
