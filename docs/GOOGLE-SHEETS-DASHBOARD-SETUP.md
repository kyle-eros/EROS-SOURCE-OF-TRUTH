# EROS Executive Dashboard - Google Sheets Setup Guide

## 🎯 Overview
This guide sets up a secure, professional executive dashboard in Google Sheets with automated BigQuery data syncing. The dashboard provides real-time insights while keeping sensitive data within Google Workspace.

## 📊 Dashboard Features
- **Executive Summary**: Key metrics, revenue analysis, growth trends
- **AI Performance**: Recommendation accuracy and performance metrics
- **Data Log**: Automatic update tracking and error logging
- **Auto-Refresh**: Updates twice daily at 8:30 AM and 6:30 PM
- **Professional Formatting**: Color-coded metrics and clean layout

## 🚀 Setup Instructions

### Step 1: Create Google Apps Script Project
1. Go to [script.google.com](https://script.google.com)
2. Click "New Project"
3. Replace the default code with the content from `google-sheets-dashboard.gs`
4. Save the project as "EROS Executive Dashboard"

### Step 2: Enable BigQuery API
1. In Apps Script, click "Services" in the left sidebar
2. Click "Add a service"
3. Select "BigQuery API" and click "Add"
4. The BigQuery service will now be available in your script

### Step 3: Set Up Permissions
1. Click the "Deploy" button, then "Test deployments"
2. Click "Authorize access" when prompted
3. Review and accept the required permissions:
   - Access to Google Sheets
   - Access to BigQuery
   - Access to Google Drive

### Step 4: Run Initial Setup
1. In Apps Script, select the `setupDashboard` function from the dropdown
2. Click the "Run" button (▶️)
3. Monitor the execution log for any errors
4. The script will create a new Google Sheet called "EROS Executive Dashboard"

### Step 5: Verify Dashboard Creation
1. Check your Google Drive for the new spreadsheet
2. Open the dashboard and verify it has three sheets:
   - Executive Summary
   - AI Performance  
   - Data Log

### Step 6: Test Data Connection
1. In Apps Script, select the `testConnection` function
2. Click "Run" to verify BigQuery connectivity
3. Check the execution log for success/error messages

### Step 7: Verify Auto-Refresh
1. The script automatically sets up twice-daily refresh triggers (8:30 AM and 6:30 PM)
2. Check "Triggers" in Apps Script to see both daily triggers
3. Manual refresh: Run the `refreshAllData` function anytime

## 📋 Dashboard Sheets Description

### Executive Summary Sheet
- **Key Metrics Cards**: Active pages, revenue, messages, conversion rates
- **Growth Analysis**: Period-over-period comparisons
- **AI Performance Summary**: Daily predictions and caption library size
- **Professional Formatting**: Color-coded sections, clear typography

### AI Performance Sheet  
- **Top 50 Recommendations**: Sorted by revenue performance
- **Accuracy Indicators**: Color-coded accuracy ratings
- **Performance Metrics**: Predicted vs actual rates, revenue impact
- **Fresh Data**: Updates twice daily after data ingestion

### Data Log Sheet
- **Update History**: Timestamps of all data refreshes
- **Error Tracking**: Automatic logging of any sync issues
- **Status Monitoring**: Success/failure status for each update
- **Record Counts**: Number of records updated per sync

## 🔧 Configuration Options

### Refresh Schedule
To change the refresh times, modify the `CONFIG` object in the script:
```javascript
const CONFIG = {
  REFRESH_TIMES: [
    { hour: 8, minute: 30 },   // Morning refresh (8:30 AM)
    { hour: 18, minute: 30 }   // Evening refresh (6:30 PM)
  ]
};
```

### Data Filters
Modify the BigQuery queries in the script to adjust:
- Date ranges (currently 30 days for executive summary)
- Record limits (currently 50 for AI performance)
- Sorting criteria

## 🛡️ Security Features
- **Internal Access Only**: Dashboard stays within Google Workspace
- **Service Account Authentication**: Secure BigQuery access
- **No Public URLs**: Data never exposed to public internet
- **Audit Trail**: All access and updates logged
- **Google Workspace Controls**: Inherits organizational security policies

## 📱 Sharing the Dashboard

### For Executives
1. Open the created Google Sheet
2. Click "Share" button
3. Add executive email addresses with "Viewer" permissions
4. Executives can access via Google Sheets mobile app

### For IT/Data Teams
1. Share with "Editor" permissions for maintenance
2. Grant access to the Apps Script project for code updates
3. Share this setup guide for troubleshooting

## 🔍 Monitoring and Maintenance

### Check Dashboard Health
1. Review the "Data Log" sheet regularly
2. Look for any "Error" status entries
3. Verify last update timestamp is recent

### Common Issues and Solutions

**Issue**: "Permission denied" errors
- **Solution**: Re-run authorization in Apps Script

**Issue**: No data appearing
- **Solution**: Check BigQuery views exist: `dashboard.v_executive_summary`

**Issue**: Trigger not running
- **Solution**: Check "Triggers" tab in Apps Script, recreate if missing

**Issue**: Slow performance
- **Solution**: Reduce refresh frequency or limit data ranges

## 🎨 Customization Options

### Branding
- Modify header colors and company name
- Add logo image to header area
- Customize color scheme to match brand

### Additional Metrics
- Add new metrics by modifying BigQuery queries
- Create additional sheets for specific departments
- Implement drill-down capabilities with filters

### Alerts and Notifications
- Add email alerts for specific thresholds
- Implement Slack notifications for major changes
- Set up mobile push notifications via Google Sheets app

## 📊 Dashboard URL
Once setup is complete, the dashboard will be available at:
`https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/`

The spreadsheet ID will be logged during setup and can be found in the Apps Script execution log.

## 💡 Best Practices
1. **Regular Monitoring**: Check the Data Log weekly
2. **Access Control**: Limit edit permissions to IT team only
3. **Backup Strategy**: Enable Google Sheets version history
4. **Performance**: Monitor BigQuery usage and costs
5. **Documentation**: Keep this setup guide updated with any customizations

## 🆘 Support and Troubleshooting
For issues with the dashboard:
1. Check the Data Log sheet for error details
2. Review Apps Script execution logs
3. Verify BigQuery views are functioning
4. Test manual data refresh first
5. Check Google Workspace admin console for API limits

## 🔄 Maintenance Schedule
- **Daily**: Monitor Data Log for errors
- **Weekly**: Verify data accuracy and completeness
- **Monthly**: Review access permissions and usage
- **Quarterly**: Update BigQuery queries if needed