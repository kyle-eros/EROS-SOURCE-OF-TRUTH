WITH all_views AS (
  SELECT
    table_schema AS dataset_id,
    table_name,
    view_definition
  FROM
    `of-scheduler-proj.mart.INFORMATION_SCHEMA.VIEWS`
  UNION ALL
  SELECT
    table_schema AS dataset_id,
    table_name,
    view_definition
  FROM
    `of-scheduler-proj.layer_04_semantic.INFORMATION_SCHEMA.VIEWS`
  UNION ALL
  SELECT
    table_schema AS dataset_id,
    table_name,
    view_definition
  FROM
    `of-scheduler-proj.layer_07_export.INFORMATION_SCHEMA.VIEWS`
)
SELECT DISTINCT
  dataset_id,
  table_name
FROM
  all_views
WHERE
  REGEXP_CONTAINS(LOWER(view_definition), r'of-scheduler-proj\.core\.')
ORDER BY
  dataset_id, table_name;