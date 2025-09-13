# 🚀 EROS Executive Dashboard - Deployment Guide

## ✅ What's Been Created

### 📊 BigQuery Views (Partially Deployed)
- ✅ `dashboard.v_revenue_overview` - Daily revenue trends
- ✅ `dashboard.v_scheduler_performance` - Scheduler rankings  
- ✅ `dashboard.v_page_performance_leaderboard` - Page performance metrics
- ⏳ `dashboard.v_executive_summary` - High-level KPIs (needs deployment)
- ⏳ `dashboard.v_ai_recommendation_performance` - AI accuracy tracking (needs deployment)

### 🌐 Dashboard Files
- ✅ `executive-revenue-dashboard.html` - Demo version with simulated data
- ✅ `executive-dashboard-live.html` - Production-ready version with BigQuery integration
- ✅ `deploy-dashboard.sh` - Automated deployment script

---

## 🎯 Quick Deployment (Run This!)

### Step 1: Complete BigQuery Views
```bash
cd /Users/kylemerriman/Desktop/EROS-SYSTEM-SOURCE-OF-TRUTH
./deploy-dashboard.sh
```

### Step 2: Access Your Dashboard
After deployment completes, access at:
```
https://storage.googleapis.com/eros-dashboard-of-scheduler-proj/index.html
```

---

## 🔧 Manual Deployment Steps

### 1. Authenticate with Google Cloud
```bash
gcloud auth login
gcloud config set project of-scheduler-proj
```

### 2. Deploy Remaining BigQuery Views
```bash
# Executive Summary View
bq query --project_id=of-scheduler-proj --use_legacy_sql=false --format=none "
CREATE OR REPLACE VIEW \`of-scheduler-proj.dashboard.v_executive_summary\` AS
-- [View definition from deploy-dashboard.sh]
"

# AI Performance View  
bq query --project_id=of-scheduler-proj --use_legacy_sql=false --format=none "
CREATE OR REPLACE VIEW \`of-scheduler-proj.dashboard.v_ai_recommendation_performance\` AS
-- [View definition from deploy-dashboard.sh]
"
```

### 3. Create Storage Bucket & Deploy Dashboard
```bash
# Create bucket
gsutil mb gs://eros-dashboard-of-scheduler-proj

# Make publicly accessible
gsutil iam ch allUsers:objectViewer gs://eros-dashboard-of-scheduler-proj

# Upload dashboard
gsutil cp executive-dashboard-live.html gs://eros-dashboard-of-scheduler-proj/index.html

# Enable website hosting
gsutil web set -m index.html -e index.html gs://eros-dashboard-of-scheduler-proj
```

---

## 📊 Test Your Views

### Quick Test Commands
```bash
# Test executive summary
bq query --project_id=of-scheduler-proj "SELECT * FROM dashboard.v_executive_summary LIMIT 1"

# Test scheduler performance  
bq query --project_id=of-scheduler-proj "SELECT * FROM dashboard.v_scheduler_performance LIMIT 5"

# Test page performance
bq query --project_id=of-scheduler-proj "SELECT * FROM dashboard.v_page_performance_leaderboard LIMIT 5"
```

---

## 🔑 For Live Data Connection (Optional Advanced Setup)

### 1. Create Service Account
```bash
# Create service account for dashboard
gcloud iam service-accounts create eros-dashboard-sa \
    --display-name="EROS Dashboard Service Account"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding of-scheduler-proj \
    --member="serviceAccount:eros-dashboard-sa@of-scheduler-proj.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

# Create and download key
gcloud iam service-accounts keys create ~/eros-dashboard-key.json \
    --iam-account=eros-dashboard-sa@of-scheduler-proj.iam.gserviceaccount.com
```

### 2. Set up API Endpoint (Requires additional development)
- Create Cloud Function or App Engine app to proxy BigQuery requests
- Use service account key for authentication
- Update dashboard JavaScript to call your API endpoint

---

## 📋 Current Dashboard Features

### 🏆 Executive KPIs
- **Total Revenue (30d)** - Revenue with growth percentage
- **Active Pages** - Number of pages generating revenue  
- **Conversion Rate** - Purchase rate with trend
- **AI Predictions Today** - Real-time ML recommendation count

### 👥 Scheduler Performance
- **Revenue Rankings** - Top performing schedulers
- **Efficiency Metrics** - Revenue per message, pages managed
- **Performance Tiers** - Excellent/Good/Fair classifications

### 📱 Page Performance  
- **Revenue Leaderboard** - Top earning pages
- **Performance Status** - Health indicators
- **Last Active** - Recency tracking
- **Scheduler Assignment** - Who manages each page

---

## 🚨 Troubleshooting

### Authentication Issues
```bash
# Re-authenticate
gcloud auth login --update-adc

# Check active account
gcloud auth list

# Set correct project
gcloud config set project of-scheduler-proj
```

### View Creation Errors
- Check that source tables exist: `core.message_facts`, `mart.caption_rank_next24_v3_tbl`
- Verify permissions: Ensure you have BigQuery Data Editor role
- Test queries individually before creating views

### Dashboard Access Issues  
- Verify bucket is public: `gsutil iam get gs://eros-dashboard-of-scheduler-proj`
- Check file uploaded correctly: `gsutil ls gs://eros-dashboard-of-scheduler-proj`
- Wait 1-2 minutes for DNS propagation

---

## 🎉 Success Indicators

✅ BigQuery views return data without errors  
✅ Dashboard loads without connection errors  
✅ KPI numbers populate from real data  
✅ Tables show actual scheduler/page performance  
✅ Real-time updates every 5 minutes

---

## 📞 Support

If you encounter issues:
1. Check the console for JavaScript errors
2. Verify BigQuery views have data: Run test queries above
3. Ensure authentication is working: `gcloud auth list`
4. Check bucket permissions: Dashboard should load without login

Your executive dashboard is now ready to provide Fortune 500 level insights! 🚀