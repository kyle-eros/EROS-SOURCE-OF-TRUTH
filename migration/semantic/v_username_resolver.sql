CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_username_resolver`
OPTIONS(description="Refactored username resolver. NOTE: This logic depends on a new, centralized username mapping/aliasing table.")
AS
-- This view cannot be fully refactored without a new source table for username aliases.
-- The query below is a placeholder structure that performs a 1-to-1 mapping.
SELECT
  username AS resolved_username_std,
  username AS alias_norm -- Placeholder, should come from alias table
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE;