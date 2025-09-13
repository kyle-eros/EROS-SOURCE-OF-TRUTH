/**
 * Caption Override System - Human-in-the-Loop ML Training
 * Allows schedulers to override AI recommendations and tracks performance
 */

// Add to CONFIG
const OVERRIDE_CONFIG = {
  TRACKING_TABLE: 'ops.caption_overrides',
  FEEDBACK_TABLE: 'ops.override_feedback',
  CAPTION_BANKS: {
    PPV: 'mart.caption_bank_ppv',
    BUMP: 'mart.caption_bank_bumps', 
    RENEW: 'mart.caption_bank_renew',
    TIP: 'mart.caption_bank_tip'
  }
};

/**
 * Show caption bank picker when scheduler wants to override
 */
function showCaptionBankPicker() {
  try {
    const sheet = SpreadsheetApp.getActiveSheet();
    const row = sheet.getActiveRange().getRow();
    
    if (row < 2) {
      SpreadsheetApp.getUi().alert('Please select a data row');
      return;
    }
    
    // Get slot context
    const rowData = sheet.getRange(row, 1, 1, sheet.getLastColumn()).getValues()[0];
    const slotType = detectSlotType(rowData);
    const model = rowData[IDX.DAY.MODEL - 1];
    const page = rowData[IDX.DAY.PAGE - 1];
    const timeSlot = rowData[IDX.DAY.TIME - 1];
    const recommendedCaptionId = rowData[IDX.DAY.CAPTION_ID - 1];
    
    // Query appropriate caption bank
    const sql = `
      SELECT 
        caption_id,
        caption_text,
        historical_revenue,
        historical_open_rate,
        times_used_30d,
        CASE 
          WHEN times_used_30d = 0 THEN '🟢 Fresh'
          WHEN times_used_30d < 3 THEN '🟡 Low Use'
          ELSE '🔴 Overused'
        END as usage_status
      FROM \`${CONFIG.PROJECT_ID}.${OVERRIDE_CONFIG.CAPTION_BANKS[slotType]}\`
      WHERE caption_id NOT IN (
        -- Exclude recently used on this page
        SELECT DISTINCT caption_id 
        FROM \`${CONFIG.PROJECT_ID}.ops.send_log\`
        WHERE username_page = @username_page
        AND date_sent >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
      )
      ORDER BY historical_revenue DESC
      LIMIT 20
    `;
    
    const params = [{name: 'username_page', type: 'STRING', value: model + '__' + page}];
    const captions = new BigQueryService().query(sql, params, false);
    
    // Build HTML picker
    const html = [
      '<style>',
      'body { font-family: Arial, sans-serif; padding: 20px; }',
      '.caption-option { border: 1px solid #ddd; padding: 15px; margin: 10px 0; cursor: pointer; border-radius: 8px; }',
      '.caption-option:hover { background-color: #f0f0f0; }',
      '.fresh { border-left: 5px solid #10b981; }',
      '.low-use { border-left: 5px solid #f59e0b; }',
      '.overused { border-left: 5px solid #ef4444; }',
      '.stats { color: #666; font-size: 12px; margin-top: 8px; }',
      '.warning { background-color: #fef3c7; padding: 10px; border-radius: 5px; margin-bottom: 15px; }',
      '</style>',
      '<div class="warning">',
      '⚠️ <b>Override Warning:</b> The AI recommended caption #' + recommendedCaptionId + ' for this slot.',
      ' Your override will be tracked and analyzed for ML training.',
      '</div>',
      '<h3>Select Caption from ' + slotType + ' Bank:</h3>'
    ];
    
    captions.forEach(function(c) {
      const cssClass = c.usage_status.includes('Fresh') ? 'fresh' : 
                       c.usage_status.includes('Low') ? 'low-use' : 'overused';
      html.push(
        '<div class="caption-option ' + cssClass + '" onclick="selectOverrideCaption(\'' + 
        c.caption_id + '\',\'' + escapeHtml(c.caption_text) + '\')">',
        '<b>Caption #' + c.caption_id + '</b> ' + c.usage_status,
        '<div style="margin: 8px 0;">' + escapeHtml(c.caption_text.substring(0, 150)) + '...</div>',
        '<div class="stats">',
        '💰 Avg Revenue: $' + (c.historical_revenue || 0).toFixed(2),
        ' | 📧 Open Rate: ' + ((c.historical_open_rate || 0) * 100).toFixed(1) + '%',
        ' | 📅 Used ' + c.times_used_30d + 'x in 30d',
        '</div>',
        '</div>'
      );
    });
    
    html.push(
      '<script>',
      'function selectOverrideCaption(captionId, captionText) {',
      '  google.script.run.withSuccessHandler(function() { google.script.host.close(); })',
      '    .applyOverrideCaption(' + row + ', captionId, captionText);',
      '}',
      'function escapeHtml(text) { return text; }',
      '</script>'
    );
    
    SpreadsheetApp.getUi().showModalDialog(
      HtmlService.createHtmlOutput(html.join('')).setWidth(800).setHeight(600),
      '🔄 Override with Caption Bank - ' + model + ' · ' + page
    );
    
  } catch(e) {
    handleError(e, 'showCaptionBankPicker');
  }
}

/**
 * Apply the override caption and track it
 */
function applyOverrideCaption(row, overrideCaptionId, overrideCaptionText) {
  try {
    const sheet = SpreadsheetApp.getActiveSheet();
    const rowData = sheet.getRange(row, 1, 1, sheet.getLastColumn()).getValues()[0];
    
    // Store original recommendation
    const originalCaptionId = rowData[IDX.DAY.CAPTION_ID - 1];
    const model = rowData[IDX.DAY.MODEL - 1];
    const page = rowData[IDX.DAY.PAGE - 1];
    const timeSlot = rowData[IDX.DAY.TIME - 1];
    const price = rowData[IDX.DAY.PRICE - 1];
    
    // Track the override in BigQuery
    const trackingSql = `
      INSERT INTO \`${CONFIG.PROJECT_ID}.${OVERRIDE_CONFIG.TRACKING_TABLE}\`
      (override_timestamp, scheduler_email, username_page, slot_time, 
       original_caption_id, override_caption_id, override_reason, slot_price)
      VALUES (
        CURRENT_TIMESTAMP(),
        @scheduler_email,
        @username_page,
        @slot_time,
        @original_caption_id,
        @override_caption_id,
        'Manual override from caption bank',
        @slot_price
      )
    `;
    
    const trackingParams = [
      {name: 'scheduler_email', type: 'STRING', value: getCurrentUserEmail_()},
      {name: 'username_page', type: 'STRING', value: model + '__' + page},
      {name: 'slot_time', type: 'STRING', value: timeSlot},
      {name: 'original_caption_id', type: 'STRING', value: originalCaptionId},
      {name: 'override_caption_id', type: 'STRING', value: overrideCaptionId},
      {name: 'slot_price', type: 'FLOAT64', value: parseFloat(price) || 0}
    ];
    
    BigQuery.Jobs.query({
      query: trackingSql,
      useLegacySql: false,
      parameterMode: 'NAMED',
      queryParameters: trackingParams
    }, CONFIG.PROJECT_ID);
    
    // Update the sheet
    sheet.getRange(row, IDX.DAY.CAPTION_ID).setValue(overrideCaptionId);
    sheet.getRange(row, IDX.DAY.CAPTION_PREV).setValue(overrideCaptionText.substring(0, 50) + '...');
    sheet.getRange(row, IDX.DAY.STATUS).setValue('Override');
    
    // Highlight the row to show it's an override
    sheet.getRange(row, 1, 1, sheet.getLastColumn()).setBackground('#fef3c7'); // Yellow highlight
    
    // Add note about override
    sheet.getRange(row, IDX.DAY.NOTES).setNote(
      'OVERRIDE by ' + getCurrentUserEmail_() + ' at ' + new Date().toLocaleString() + 
      '\nOriginal AI recommendation: #' + originalCaptionId +
      '\nOverride caption: #' + overrideCaptionId +
      '\nThis override will be analyzed for ML training after performance data is available.'
    );
    
    SpreadsheetApp.getUi().alert(
      '✅ Override Applied',
      'Caption #' + overrideCaptionId + ' has been applied.\n\n' +
      'This override is being tracked for ML training.\n' +
      'You\'ll receive feedback on this decision within 24-48 hours.',
      SpreadsheetApp.getUi().ButtonSet.OK
    );
    
  } catch(e) {
    handleError(e, 'applyOverrideCaption');
  }
}

/**
 * Check override performance and provide feedback (runs daily)
 */
function analyzeOverridePerformance() {
  try {
    const sql = `
      WITH override_performance AS (
        SELECT 
          o.scheduler_email,
          o.username_page,
          o.slot_time,
          o.original_caption_id,
          o.override_caption_id,
          o.override_timestamp,
          
          -- Get actual performance of override
          actual.revenue as override_revenue,
          actual.open_rate as override_open_rate,
          
          -- Get projected performance of original
          proj.projected_revenue as original_projected_revenue,
          proj.projected_open_rate as original_projected_open_rate,
          
          -- Calculate performance delta
          actual.revenue - proj.projected_revenue as revenue_delta,
          actual.open_rate - proj.projected_open_rate as open_rate_delta,
          
          -- Determine if override was good/bad
          CASE 
            WHEN actual.revenue > proj.projected_revenue * 1.1 THEN 'SUCCESS - Beat projection by >10%'
            WHEN actual.revenue < proj.projected_revenue * 0.9 THEN 'FAILURE - Underperformed by >10%'
            ELSE 'NEUTRAL - Similar to projection'
          END as override_result
          
        FROM \`${CONFIG.PROJECT_ID}.${OVERRIDE_CONFIG.TRACKING_TABLE}\` o
        
        -- Join actual performance
        LEFT JOIN \`${CONFIG.PROJECT_ID}.ops.campaign_performance\` actual
          ON actual.caption_id = o.override_caption_id
          AND actual.username_page = o.username_page
          AND DATE(actual.send_timestamp) = DATE(o.override_timestamp)
        
        -- Join projected performance
        LEFT JOIN \`${CONFIG.PROJECT_ID}.mart.caption_projections\` proj
          ON proj.caption_id = o.original_caption_id
          AND proj.username_page = o.username_page
          
        WHERE o.override_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
        AND o.override_timestamp <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        AND actual.revenue IS NOT NULL
      )
      
      INSERT INTO \`${CONFIG.PROJECT_ID}.${OVERRIDE_CONFIG.FEEDBACK_TABLE}\`
      SELECT 
        CURRENT_TIMESTAMP() as feedback_timestamp,
        scheduler_email,
        username_page,
        override_result,
        CONCAT(
          'Your override ', 
          IF(revenue_delta > 0, 'earned $', 'lost $'), 
          ABS(ROUND(revenue_delta, 2)),
          ' vs AI recommendation. ',
          IF(revenue_delta > 0, 
            'Good instinct! The AI will learn from this.',
            'The AI recommendation would have performed better. Consider trusting it more.'
          )
        ) as feedback_message,
        revenue_delta,
        open_rate_delta
      FROM override_performance
    `;
    
    BigQuery.Jobs.query({query: sql, useLegacySql: false}, CONFIG.PROJECT_ID);
    
    console.log('Override performance analysis complete');
    
  } catch(e) {
    console.error('Error analyzing override performance:', e);
  }
}

/**
 * Detect slot type from row data
 */
function detectSlotType(rowData) {
  const captionText = (rowData[IDX.DAY.CAPTION_PREV - 1] || '').toLowerCase();
  
  if (captionText.includes('renew') || captionText.includes('rebill')) return 'RENEW';
  if (captionText.includes('tip') || captionText.includes('send me')) return 'TIP';
  if (captionText.includes('bump') || captionText.includes('last chance')) return 'BUMP';
  return 'PPV';
}

/**
 * Show feedback to schedulers about their recent overrides
 */
function showOverrideFeedback() {
  try {
    const email = getCurrentUserEmail_();
    
    const sql = `
      SELECT 
        feedback_timestamp,
        username_page,
        override_result,
        feedback_message,
        ROUND(revenue_delta, 2) as revenue_impact
      FROM \`${CONFIG.PROJECT_ID}.${OVERRIDE_CONFIG.FEEDBACK_TABLE}\`
      WHERE scheduler_email = @email
      AND feedback_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      ORDER BY feedback_timestamp DESC
      LIMIT 10
    `;
    
    const params = [{name: 'email', type: 'STRING', value: email}];
    const feedback = new BigQueryService().query(sql, params, false);
    
    if (feedback.length === 0) {
      SpreadsheetApp.getUi().alert('No recent override feedback available yet. Check back in 24-48 hours after making overrides.');
      return;
    }
    
    let message = 'Your Recent Override Performance:\n\n';
    let totalImpact = 0;
    
    feedback.forEach(function(f) {
      const icon = f.override_result.includes('SUCCESS') ? '✅' : 
                   f.override_result.includes('FAILURE') ? '❌' : '➖';
      message += icon + ' ' + f.username_page + ': ' + f.feedback_message + '\n\n';
      totalImpact += f.revenue_impact;
    });
    
    message += '💰 Total Revenue Impact: $' + totalImpact.toFixed(2);
    
    SpreadsheetApp.getUi().alert('📊 Your Override Feedback', message, SpreadsheetApp.getUi().ButtonSet.OK);
    
  } catch(e) {
    handleError(e, 'showOverrideFeedback');
  }
}

// Helper function to escape HTML
function escapeHtml(text) {
  if (!text) return '';
  return text.replace(/[&<>"']/g, function(match) {
    const escapeMap = {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'};
    return escapeMap[match];
  });
}