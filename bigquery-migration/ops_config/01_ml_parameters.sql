-- =========================================
-- OPS CONFIG: ML Parameters
-- =========================================
-- Purpose: Centralized configuration for ML ranking
-- JSON-based for flexibility with versioning
-- =========================================

CREATE OR REPLACE TABLE `of-scheduler-proj.ops_config.ml_parameters` (
  config_id STRING NOT NULL,
  environment STRING NOT NULL,  -- 'dev', 'staging', 'prod'
  page_state STRING,            -- 'active', 'dormant', 'new'
  
  -- JSON configuration for maximum flexibility
  parameters JSON NOT NULL,
  
  -- Versioning
  version_number INT64 NOT NULL,
  is_active BOOLEAN NOT NULL,
  
  -- A/B testing support
  experiment_id STRING,
  experiment_allocation FLOAT64,  -- Percentage of traffic (0-1)
  
  -- Metadata
  created_by STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  expires_at TIMESTAMP,
  description STRING,
  
  -- Primary key
  PRIMARY KEY (config_id) NOT ENFORCED
);

-- Insert default configurations
INSERT INTO `of-scheduler-proj.ops_config.ml_parameters` 
(config_id, environment, page_state, parameters, version_number, is_active, created_by, created_at, description)
VALUES
-- Production config for active pages
('prod_active_v1', 'prod', 'active', JSON'''
{
  "weights": {
    "performance": 0.4,
    "exploration": 0.2,
    "recency": 0.2,
    "stability": 0.2
  },
  "exploration": {
    "epsilon_percent": 10,
    "ucb_c": 2.0,
    "new_caption_boost": 1.5,
    "min_samples_for_exploitation": 10
  },
  "cooldown": {
    "min_hours": 6,
    "base_hours": 6,
    "backoff_factor": 1.5,
    "max_daily_uses": 3,
    "fatigue_threshold": 0.8
  },
  "thresholds": {
    "min_confidence": 0.3,
    "min_rps_for_promotion": 0.5,
    "max_days_inactive": 30
  },
  "bayesian_priors": {
    "prior_sends": 100,
    "prior_purchase_rate": 0.05,
    "prior_rps": 0.3
  }
}
''', 1, TRUE, 'system', CURRENT_TIMESTAMP(), 'Default production config for active pages'),

-- Config for new/dormant pages (more exploration)
('prod_new_v1', 'prod', 'new', JSON'''
{
  "weights": {
    "performance": 0.2,
    "exploration": 0.4,
    "recency": 0.2,
    "stability": 0.2
  },
  "exploration": {
    "epsilon_percent": 25,
    "ucb_c": 3.0,
    "new_caption_boost": 2.0,
    "min_samples_for_exploitation": 20
  },
  "cooldown": {
    "min_hours": 4,
    "base_hours": 4,
    "backoff_factor": 1.3,
    "max_daily_uses": 5,
    "fatigue_threshold": 0.9
  },
  "thresholds": {
    "min_confidence": 0.1,
    "min_rps_for_promotion": 0.3,
    "max_days_inactive": 60
  },
  "bayesian_priors": {
    "prior_sends": 50,
    "prior_purchase_rate": 0.03,
    "prior_rps": 0.2
  }
}
''', 1, TRUE, 'system', CURRENT_TIMESTAMP(), 'High exploration config for new pages'),

-- A/B test config (experimental)
('experiment_aggressive_v1', 'prod', 'active', JSON'''
{
  "weights": {
    "performance": 0.6,
    "exploration": 0.1,
    "recency": 0.1,
    "stability": 0.2
  },
  "exploration": {
    "epsilon_percent": 5,
    "ucb_c": 1.0,
    "new_caption_boost": 1.0,
    "min_samples_for_exploitation": 5
  },
  "cooldown": {
    "min_hours": 8,
    "base_hours": 8,
    "backoff_factor": 2.0,
    "max_daily_uses": 2,
    "fatigue_threshold": 0.7
  },
  "thresholds": {
    "min_confidence": 0.5,
    "min_rps_for_promotion": 0.7,
    "max_days_inactive": 14
  },
  "bayesian_priors": {
    "prior_sends": 200,
    "prior_purchase_rate": 0.07,
    "prior_rps": 0.4
  }
}
''', 1, TRUE, 'system', CURRENT_TIMESTAMP(), 'Aggressive exploitation for A/B test');

-- Create view for easy access to active configs
CREATE OR REPLACE VIEW `of-scheduler-proj.ops_config.active_ml_config` AS
SELECT
  page_state,
  environment,
  
  -- Extract key parameters for easy access
  JSON_VALUE(parameters, '$.weights.performance') AS w_performance,
  JSON_VALUE(parameters, '$.weights.exploration') AS w_exploration,
  JSON_VALUE(parameters, '$.weights.recency') AS w_recency,
  JSON_VALUE(parameters, '$.weights.stability') AS w_stability,
  
  JSON_VALUE(parameters, '$.exploration.epsilon_percent') AS epsilon_percent,
  JSON_VALUE(parameters, '$.exploration.ucb_c') AS ucb_c,
  
  JSON_VALUE(parameters, '$.cooldown.min_hours') AS min_cooldown_hours,
  JSON_VALUE(parameters, '$.cooldown.fatigue_threshold') AS fatigue_threshold,
  
  -- Full JSON for complex logic
  parameters AS full_config,
  
  -- Metadata
  version_number,
  experiment_id,
  experiment_allocation,
  created_at,
  expires_at
  
FROM `of-scheduler-proj.ops_config.ml_parameters`
WHERE is_active = TRUE
  AND environment = 'prod'
  AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP());