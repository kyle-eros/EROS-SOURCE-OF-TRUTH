WITH all_routines AS (
  SELECT
    routine_schema AS dataset_id,
    routine_name,
    routine_definition
  FROM
    `of-scheduler-proj.mart.INFORMATION_SCHEMA.ROUTINES`
  UNION ALL
  SELECT
    routine_schema AS dataset_id,
    routine_name,
    routine_definition
  FROM
    `of-scheduler-proj.layer_04_semantic.INFORMATION_SCHEMA.ROUTINES`
  UNION ALL
  SELECT
    routine_schema AS dataset_id,
    routine_name,
    routine_definition
  FROM
    `of-scheduler-proj.core.INFORMATION_SCHEMA.ROUTINES`
)
SELECT DISTINCT
  dataset_id,
  routine_name
FROM
  all_routines
WHERE
  REGEXP_CONTAINS(LOWER(routine_definition), r'of-scheduler-proj\.core\.')
ORDER BY
  dataset_id, routine_name;