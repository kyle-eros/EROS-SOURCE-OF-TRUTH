-- Check mart dataset routines for core dependencies
SELECT
  routine_catalog,
  routine_schema AS dataset_id,
  routine_name,
  routine_type
FROM `of-scheduler-proj.mart`.INFORMATION_SCHEMA.ROUTINES
WHERE REGEXP_CONTAINS(LOWER(routine_definition), r'of-scheduler-proj\.core\.')
UNION ALL
-- Check layer_04_semantic dataset routines for core dependencies
SELECT
  routine_catalog,
  routine_schema AS dataset_id,
  routine_name,
  routine_type
FROM `of-scheduler-proj.layer_04_semantic`.INFORMATION_SCHEMA.ROUTINES
WHERE REGEXP_CONTAINS(LOWER(routine_definition), r'of-scheduler-proj\.core\.')
UNION ALL
-- Check ops_config dataset routines for core dependencies
SELECT
  routine_catalog,
  routine_schema AS dataset_id,
  routine_name,
  routine_type
FROM `of-scheduler-proj.ops_config`.INFORMATION_SCHEMA.ROUTINES
WHERE REGEXP_CONTAINS(LOWER(routine_definition), r'of-scheduler-proj\.core\.')
ORDER BY 2, 3;