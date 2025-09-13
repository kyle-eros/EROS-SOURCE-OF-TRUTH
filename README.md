# 🚀 EROS System - Source of Truth

## Overview
Enterprise Resource Optimization System for OnlyFans scheduling and content management. This repository contains the complete ML-powered scheduling system with Google Sheets UI for human schedulers.

## 📁 Project Structure

```
EROS-SYSTEM-SOURCE-OF-TRUTH/
├── apps-script/           # Google Apps Script configuration
│   ├── .clasp.json       # Clasp deployment config
│   └── appsscript.json   # Apps Script manifest
│
├── config/               # System configuration files
│
├── data/                 # Data processing and storage
│   ├── captions/        # Caption banks and templates
│   └── models/          # ML model outputs
│
├── docs/                 # Documentation
│   ├── CAPTION_BANKS_GUIDE.md      # Caption bank column guide
│   ├── SCHEDULER_TEAM_SETUP.md     # Scheduler onboarding
│   ├── SIMPLE_SCHEDULER_GUIDE.md   # Simple user guide
│   └── *.md                         # Other documentation
│
├── monitoring/           # System monitoring and alerts
│   └── alerts/          # Alert configurations
│
├── reports/              # Generated reports and analytics
│
├── scripts/              # Utility scripts
│   ├── run_setup.sh     # Apps Script setup automation
│   └── test_scheduler_setup.sh  # System verification
│
├── sql-views/            # BigQuery SQL definitions
│   ├── create_override_tables.sql  # Override tracking
│   └── *.sql            # View definitions
│
└── src/                  # Source code
    ├── google-apps-script/  # Google Sheets UI code
    │   ├── Main.js          # Core scheduler logic
    │   ├── Setup.js         # Automated setup
    │   ├── CaptionBanks.js # Caption bank management
    │   └── CaptionOverride.js # Override & ML feedback
    │
    ├── python-etl/          # Python ETL pipelines
    │   └── *.py            # Data processing scripts
    │
    └── sql/                 # SQL queries and procedures
```

## 🎯 Key Components

### Google Sheets Scheduler UI
- **Location**: `src/google-apps-script/`
- **Purpose**: Human-friendly interface for schedulers
- **Features**: AI recommendations, caption banks, override tracking

### BigQuery Data Pipeline
- **Location**: `sql-views/`
- **Purpose**: Data warehouse and ML training
- **Project**: `of-scheduler-proj`

### Python ETL
- **Location**: `src/python-etl/`
- **Purpose**: Data ingestion and processing
- **Includes**: Gmail ETL, performance tracking

## 🚀 Quick Start

### For Schedulers
1. Open the Google Sheet (link provided by admin)
2. Click "Load My Day" to see schedule
3. Follow the recommendations
4. See `docs/SIMPLE_SCHEDULER_GUIDE.md`

### For Admins
1. Run `scripts/run_setup.sh`
2. Open Apps Script project
3. Run `runCompleteSetup` function
4. Share spreadsheet with team
5. See `docs/SCHEDULER_TEAM_SETUP.md`

### For Developers
1. Install clasp: `npm install -g @google/clasp`
2. Authenticate: `clasp login`
3. Push changes: `clasp push`
4. Deploy: `clasp deploy`

## 📊 System Architecture

```
[Gmail] → [Python ETL] → [BigQuery] → [ML Pipeline]
                              ↓
                    [Google Sheets UI] ← [Schedulers]
                              ↓
                      [Performance Tracking]
                              ↓
                        [ML Training Loop]
```

## 🔑 Key Features

- **AI-Powered Recommendations**: ML model suggests optimal captions
- **Human Override System**: Schedulers can override with tracking
- **Performance Feedback**: Learn from successes and failures
- **Caption Banks**: Organized libraries by content type
- **28-Day Cooldown**: Automatic caption rotation
- **Real-time Sync**: BigQuery integration for live data

## 📈 Performance Metrics

- 505,842 captions in library
- 832 daily slot recommendations
- 28-day cooldown enforcement
- Top-10 caption suggestions per slot
- ML training from human overrides

## 🛠️ Technologies

- **Frontend**: Google Apps Script, Google Sheets
- **Backend**: BigQuery, Cloud Functions
- **ML**: TensorFlow, Vertex AI
- **ETL**: Python, Apache Beam
- **Deployment**: Cloud Build, clasp

## 📝 Documentation

- [Simple Guide](docs/SIMPLE_SCHEDULER_GUIDE.md) - For daily users
- [Setup Guide](docs/SCHEDULER_TEAM_SETUP.md) - For initial setup
- [Caption Banks](docs/CAPTION_BANKS_GUIDE.md) - Column explanations
- [ML Architecture](docs/ML_SCHEDULING_OPTIMIZATIONS_DEPLOYMENT_GUIDE.md) - Technical details

## 🔒 Security

- Google Workspace authentication
- BigQuery row-level security
- Encrypted API keys
- Audit logging to `ops.send_log`

## 🆘 Support

- **Scheduler Issues**: Check activity log in Google Sheets
- **Data Issues**: Run `scripts/test_scheduler_setup.sh`
- **Code Issues**: Check Apps Script execution logs

## 📅 Version History

- **v7.2** - Added caption banks and override system
- **v7.1** - Page-aware recommendations
- **v7.0** - ML integration complete
- **v6.0** - BigQuery migration

## 🎖️ License

Proprietary - All rights reserved

---

*Last Updated: September 2025*
*System Status: ✅ Operational*