# ML Caption Scheduling System Documentation

## Overview
Production-grade ML system for intelligent caption scheduling with exploration/exploitation balance, temporal optimization, and real-time monitoring.

## Architecture

### Data Flow
```
RAW DATA → STAGING → FOUNDATION → SEMANTIC → ML → OPTIMIZATION → EXPORT
```

### Datasets
1. **layer_01_raw**: Source data from uploads
2. **layer_02_staging**: Cleaned and standardized data
3. **layer_03_foundation**: Core dimensions and facts
4. **layer_04_semantic**: Business metrics and aggregations
5. **layer_05_ml**: Feature store and ML inputs
6. **layer_06_optimization**: ML rankings and recommendations
7. **layer_07_export**: API-ready views
8. **ops_config**: System configuration
9. **ops_monitor**: Monitoring and alerting

## Key Components

### 1. Feature Store (`layer_05_ml.feature_store`)
- **Incremental Updates**: Daily refresh with DELETE + INSERT pattern
- **Volume-Weighted Metrics**: True RPS calculations using ratio-of-sums
- **Temporal Intelligence**: Best hour/day patterns without correlated subqueries
- **Bayesian Smoothing**: Configurable priors for small sample sizes

Key Features:
- Performance features (RPS, confidence scores)
- Temporal features (best hours, day patterns)
- Cooldown features (eligibility, fatigue)
- Exploration features (UCB, novelty bonuses)
- Composite scores with configurable weights

### 2. ML Ranker (`layer_06_optimization.ml_caption_ranker`)
- **Multi-Armed Bandit**: UCB + epsilon-greedy exploration
- **Dynamic Cooldowns**: Exponential backoff based on usage
- **Personalized Ranking**: Per creator-page combinations
- **Strategy Support**: Explore/balanced/exploit modes

### 3. Configuration System (`ops_config`)
- **JSON-based parameters**: Flexible configuration without schema changes
- **A/B testing support**: Multiple configs with traffic allocation
- **Environment-specific**: Dev/staging/prod configurations
- **Page state awareness**: Different strategies for new vs established pages

## Algorithms

### Exploration vs Exploitation
```python
# Epsilon-greedy with UCB
if random() < epsilon:
    select_random_caption()  # Explore
else:
    select_by_score()        # Exploit

# UCB bonus for uncertainty
ucb_bonus = c * sqrt(ln(total_sends) / caption_sends)
```

### Cooldown Algorithm
```python
# Dynamic cooldown with exponential backoff
base_cooldown = 6 hours
backoff_factor = 1.5
consecutive_uses_today = count_uses_today(caption)

required_cooldown = base_cooldown * (backoff_factor ^ consecutive_uses_today)
```

### Volume-Weighted Averaging
```sql
-- Correct RPS calculation
SUM(revenue) / SUM(sends) AS rps_weighted
-- NOT: AVG(revenue/sends) which gives equal weight
```

## Daily Operations

### Automated Refresh
```bash
# Scheduled via cron at 2 AM UTC
./bigquery-migration/scripts/daily_ml_refresh.sh

# Manual refresh
./bigquery-migration/scripts/daily_ml_refresh.sh
```

### Monitoring
Access monitoring dashboards:
```sql
SELECT * FROM `of-scheduler-proj.ops_monitor.dashboard_system_health`
SELECT * FROM `of-scheduler-proj.ops_monitor.dashboard_ml_performance`
SELECT * FROM `of-scheduler-proj.ops_monitor.dashboard_alerts`
```

### Testing
```bash
# Run end-to-end tests
./bigquery-migration/tests/test_ml_pipeline.sh

# Check specific component
bq query --use_legacy_sql=false "SELECT * FROM ops_monitor.dashboard_system_health"
```

## Apps Script Integration

### Functions
```javascript
// Get ML-ranked captions
=getCaptions(username, page_type, num_captions, strategy)

// Check system health
=getSystemHealth()

// View top performers
=getTopCaptions(limit)

// Lookup specific caption
=lookupCaption(caption_id)
```

### Strategies
- **explore**: 25% random selection
- **balanced**: 10% random selection (default)
- **exploit**: 5% random selection

## Configuration

### ML Parameters
Edit `ops_config.ml_parameters` to adjust:
- Component weights (performance, exploration, recency, stability)
- Exploration parameters (epsilon, UCB constant)
- Cooldown settings (min hours, backoff factor)
- Thresholds (confidence, RPS minimums)
- Bayesian priors

### Business Rules
Edit `ops_config.business_rules` to control:
- Max daily sends per caption
- Minimum cooldown periods
- Required confidence thresholds
- Page-specific overrides

## Troubleshooting

### Common Issues

1. **Stale Data Alert**
   - Check cron job: `crontab -l`
   - Manual refresh: `./scripts/daily_ml_refresh.sh`
   - Check logs: `tail -f logs/ml_refresh_*.log`

2. **Low Eligibility Rate**
   - Review cooldown settings in config
   - Check fatigue scores
   - Verify temporal patterns

3. **Poor Performance**
   - Check data quality dashboard
   - Review exploration/exploitation balance
   - Verify volume-weighted calculations

### Debug Queries
```sql
-- Check feature store freshness
SELECT MAX(computed_at) FROM `layer_05_ml.feature_store`;

-- Review eligible captions
SELECT COUNT(*), AVG(CASE WHEN cooldown_features.is_eligible THEN 1 ELSE 0 END)
FROM `layer_05_ml.feature_store`
WHERE computed_date = CURRENT_DATE();

-- Check specific user recommendations
SELECT * FROM `layer_07_export.schedule_recommendations`
WHERE username_page = 'USER_PAGE'
AND schedule_date = CURRENT_DATE()
ORDER BY final_score DESC;
```

## Performance Metrics

### Target SLAs
- Feature store refresh: < 5 minutes
- Query response time: < 2 seconds
- Data freshness: < 26 hours
- Eligibility rate: > 30%

### Key KPIs
- Average RPS across system
- Exploration ratio (new vs proven captions)
- Cooldown effectiveness (fatigue prevention)
- Temporal optimization lift

## Migration Notes

### From Old System
- Caption IDs preserved for continuity
- Historical performance data maintained
- Gradual transition via A/B testing
- Rollback procedures in place

### Backup Strategy
```bash
# Create backup before changes
./bigquery-migration/00_backup/backup_all.sh

# Restore if needed
./bigquery-migration/00_backup/restore_from_backup.sh
```

## Support

### Monitoring Links
- System Health: `ops_monitor.dashboard_system_health`
- Performance Tracking: `ops_monitor.dashboard_ml_performance`
- Data Quality: `ops_monitor.dashboard_data_quality`
- Alerts: `ops_monitor.dashboard_alerts`

### Contact
For issues or questions about the ML system, check:
1. Monitoring dashboards
2. System logs in `bigquery-migration/logs/`
3. Test results in `bigquery-migration/tests/`

## Next Steps

### Planned Improvements
1. Real-time feature updates
2. Multi-objective optimization
3. Reinforcement learning integration
4. Advanced temporal modeling
5. Cross-page caption sharing

### Experimentation Framework
- A/B test infrastructure ready
- Config-driven experiments
- Automated metrics tracking
- Statistical significance testing