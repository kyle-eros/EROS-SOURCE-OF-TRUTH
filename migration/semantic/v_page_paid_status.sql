CREATE OR REPLACE VIEW `of-scheduler-proj.layer_04_semantic.v_page_paid_status`
OPTIONS(description="Refactored view to determine if a page is a paid subscription page, using the new creator dimension.")
AS
SELECT
  username AS username_std,
  -- This logic is a placeholder. The legacy `renew_on_pct > 0` logic
  -- needs a stable source in the new architecture, likely from `creator_stats_latest`.
  -- For now, we infer from account type.
  CASE
    WHEN account_type = 'vip' THEN TRUE
    ELSE FALSE
  END AS is_paid_sub,
  last_active_date AS decided_as_of
FROM `of-scheduler-proj.layer_03_foundation.dim_creator`
WHERE is_current_record = TRUE;