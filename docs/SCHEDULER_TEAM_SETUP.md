# 🚀 EROS Scheduler Hub - Quick Setup Guide

## ✅ System Status
- **BigQuery**: ✅ Connected and operational
- **Caption Recommendations**: ✅ 832 slots available
- **Data Pipeline**: ✅ All views and tables verified
- **Apps Script**: ✅ Code deployed (v7.1)

## 🔗 Important Links

### 1. Apps Script Project (Admin Only)
https://script.google.com/d/1gCc7UoVcMDLOKiDUblKMbozB8bVGrvIZQp1FXTij6OFwNzvJ2HexnSE0/edit

### 2. Google Sheet (Will be shared after setup)
*Link will be provided after initial setup*

## 📋 Setup Steps (For Admin)

### Step 1: Initialize the Spreadsheet
1. Open the Apps Script project link above
2. In the editor, select `onOpen` from the dropdown menu
3. Click the ▶️ Run button
4. When prompted, authorize the script to access:
   - Google Sheets
   - BigQuery
   - Google Drive

### Step 2: Authorize BigQuery Access
1. Select `authorizeOnce` from the dropdown
2. Click ▶️ Run
3. Accept any additional permissions if prompted

### Step 3: Create/Link the Spreadsheet
1. After running `onOpen`, a spreadsheet will be created
2. Find it in your Google Drive: "EROS Scheduler Hub"
3. Open the spreadsheet to verify these sheets exist:
   - 📅 Week Plan
   - ✅ My Day
   - 📋 Brief
   - ⚠️ Alerts
   - ⚙️ Settings
   - 📝 Activity Log

### Step 4: Test Core Functions
1. In the spreadsheet, you'll see the "🚀 Scheduler Hub" menu
2. Test these functions:
   - **📅 Refresh Week Plan**: Loads weekly schedule
   - **✅ Load My Day**: Loads today's schedule
   - **✅ Load My Day (All)**: Loads complete daily view

## 👥 For Schedulers

### How to Use the Scheduler Hub

1. **Access the Sheet**
   - You'll receive a link to the Google Sheet
   - Bookmark it for easy access
   - Available on mobile via Google Sheets app

2. **Daily Workflow**
   - Click **🚀 Scheduler Hub** menu
   - Select **✅ Load My Day** to see today's schedule
   - Review recommended captions and times
   - Use **🧠 Pick caption for row** to select different captions

3. **Key Features**
   - **Smart Caption Recommendations**: AI-powered caption suggestions
   - **Cooldown Management**: Automatic 28-day caption rotation
   - **Real-time Updates**: Syncs with BigQuery every action
   - **Activity Logging**: All changes tracked automatically

4. **Color Coding**
   - 🟢 Green: Recommended/Optimal
   - 🟡 Yellow: Modified by scheduler
   - 🔵 Blue: Sent/Completed
   - 🔴 Red: Alert/Needs attention
   - ⚪ Gray: Locked/System managed

## 🎯 Key Functions

### For Daily Scheduling
- **Load My Day**: Shows PPV schedule for today
- **Load My Day (All)**: Shows PPV + Follow-ups + DripSet + Renewals
- **Pick Caption**: Interactive caption selector with top 10 recommendations

### For Planning
- **Refresh Week Plan**: 7-day schedule overview
- **Generate Brief**: Performance summary and insights
- **Check Alerts**: Important notifications and warnings

### For Submission
- **Submit Daily Plan**: Logs completed schedule to BigQuery
- **View Activity Log**: See all recent actions and changes

## ⚠️ Important Notes

1. **Do NOT edit the Apps Script code** unless you know what you're doing
2. **Caption recommendations update hourly** - always use latest data
3. **All actions are logged** for compliance and optimization
4. **The system enforces a 28-day cooldown** on caption reuse
5. **Page-aware recommendations** - each page gets optimized captions

## 🆘 Troubleshooting

### If you see "No data"
- Click **🚀 Scheduler Hub** → **📅 Refresh Week Plan**
- Wait 5-10 seconds for data to load

### If captions won't load
- Check your internet connection
- Try **🔄 Sync with BigQuery**
- Contact admin if issue persists

### If you get permission errors
- Make sure you're logged into the correct Google account
- Ask admin to verify your access permissions

## 📊 Current System Metrics
- **Active Caption Library**: 505,842 captions
- **Daily Recommendations**: 832 slots
- **Cooldown Period**: 28 days
- **Top-N Display**: 10 captions per slot

## 🚦 Ready to Share!

The system is fully operational and ready for the scheduler team. 

**Next Steps:**
1. Complete the admin setup steps above
2. Get the spreadsheet link from Google Drive
3. Share with schedulers (Viewer or Editor access as needed)
4. Send them this guide

---

*System Version: 7.1 | BigQuery Project: of-scheduler-proj*
*Last Updated: Sep 10, 2025*