SELECT REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\.core\.') AS has_core
FROM `of-scheduler-proj.mart.INFORMATION_SCHEMA.VIEWS`
WHERE table_name = 'v_page_volume_profile_28d';
