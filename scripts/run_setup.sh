#!/bin/bash

echo "🚀 EROS Scheduler Hub - Automated Setup"
echo "======================================"
echo ""

# Open the Apps Script editor
echo "📝 Opening Apps Script editor..."
echo "URL: https://script.google.com/d/1gCc7UoVcMDLOKiDUblKMbozB8bVGrvIZQp1FXTij6OFwNzvJ2HexnSE0/edit"
echo ""

# Instructions for manual execution
echo "⚠️  IMPORTANT: Manual steps required:"
echo ""
echo "1. Click the link above to open Apps Script"
echo "2. In the editor, find 'runCompleteSetup' in the function dropdown"
echo "3. Click the ▶️ Run button"
echo "4. Authorize when prompted (accept all permissions)"
echo "5. Check the execution log for the spreadsheet URL"
echo ""
echo "Alternative: Run these functions in order:"
echo "  - onOpen (creates sheets and menu)"
echo "  - authorizeOnce (sets up BigQuery)"
echo "  - refreshWeekPlan (loads initial data)"
echo ""

# Try to open the script in browser
if command -v open &> /dev/null; then
    echo "🌐 Opening Apps Script in your browser..."
    open "https://script.google.com/d/1gCc7UoVcMDLOKiDUblKMbozB8bVGrvIZQp1FXTij6OFwNzvJ2HexnSE0/edit"
elif command -v xdg-open &> /dev/null; then
    xdg-open "https://script.google.com/d/1gCc7UoVcMDLOKiDUblKMbozB8bVGrvIZQp1FXTij6OFwNzvJ2HexnSE0/edit"
fi

echo ""
echo "📊 After setup completes:"
echo "1. Copy the spreadsheet URL from the logs"
echo "2. Share with your scheduler team"
echo "3. Send them the SCHEDULER_TEAM_SETUP.md guide"
echo ""
echo "✨ Ready for manual execution!"