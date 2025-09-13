SELECT m.*
FROM `of-scheduler-proj.layer_04_semantic.message_facts` m
JOIN `of-scheduler-proj.layer_04_semantic.v_page_dim` p USING (username_std)
WHERE m.sending_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
