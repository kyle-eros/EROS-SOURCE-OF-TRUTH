CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_username_dim`
OPTIONS(description="Refactored username dimension. NOTE: This logic depends on a new, centralized username mapping/aliasing table.")
AS
-- This view cannot be fully refactored without a new source table for username aliases.
-- The query below is a placeholder structure that performs a 1-to-1 mapping.
SELECT
  username AS username_std,
  username AS username_raw -- Placeholder, should come from alias table
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE;