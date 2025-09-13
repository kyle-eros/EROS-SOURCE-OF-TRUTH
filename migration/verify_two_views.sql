SELECT table_name,
       REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\.core\.') AS has_core
FROM `of-scheduler-proj.mart.INFORMATION_SCHEMA.VIEWS`
WHERE table_name IN ('v_slot_recommendations_next24_v3','v_caption_candidate_pool_v3');