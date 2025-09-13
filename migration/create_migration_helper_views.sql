-- Create helper views in migration dataset that mirror the layer_05_ml views
-- These are needed for the mart views to work

CREATE OR REPLACE VIEW `of-scheduler-proj.migration.v_caption_safe_candidates` AS
SELECT * FROM `of-scheduler-proj.layer_05_ml.v_caption_safe_candidates`;

CREATE OR REPLACE VIEW `of-scheduler-proj.migration.v_caption_last_used` AS
SELECT * FROM `of-scheduler-proj.layer_05_ml.v_caption_last_used`;