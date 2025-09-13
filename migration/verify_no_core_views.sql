-- Check mart dataset views for core dependencies
SELECT
  table_catalog,
  table_schema AS dataset_id,
  table_name
FROM `of-scheduler-proj.mart`.INFORMATION_SCHEMA.VIEWS
WHERE REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\.core\.')
UNION ALL
-- Check layer_04_semantic dataset views for core dependencies
SELECT
  table_catalog,
  table_schema AS dataset_id,
  table_name
FROM `of-scheduler-proj.layer_04_semantic`.INFORMATION_SCHEMA.VIEWS
WHERE REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\.core\.')
UNION ALL
-- Check layer_05_ml dataset views for core dependencies
SELECT
  table_catalog,
  table_schema AS dataset_id,
  table_name
FROM `of-scheduler-proj.layer_05_ml`.INFORMATION_SCHEMA.VIEWS
WHERE REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\.core\.')
ORDER BY 2, 3;