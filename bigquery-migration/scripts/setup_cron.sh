#!/bin/bash

# =========================================
# Setup Cron Job for Daily ML Refresh
# =========================================

SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/daily_ml_refresh.sh"

echo "Setting up cron job for daily ML refresh..."

# Make the script executable
chmod +x "$SCRIPT_PATH"

# Add to crontab (runs at 2 AM UTC daily)
# Note: This appends to existing crontab
(crontab -l 2>/dev/null; echo "0 2 * * * $SCRIPT_PATH") | crontab -

echo "✓ Cron job added successfully!"
echo ""
echo "Current crontab:"
crontab -l

echo ""
echo "The ML pipeline will refresh daily at 2:00 AM UTC"
echo "Logs will be saved to: $(dirname "$SCRIPT_PATH")/../logs/"
echo ""
echo "To monitor the cron job:"
echo "  - View crontab: crontab -l"
echo "  - Edit crontab: crontab -e"
echo "  - Remove job: crontab -r"
echo "  - Check logs: tail -f $(dirname "$SCRIPT_PATH")/../logs/ml_refresh_*.log"