SELECT table_name
FROM `of-scheduler-proj.mart.INFORMATION_SCHEMA.VIEWS`
WHERE REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\.core\.')
ORDER BY table_name;