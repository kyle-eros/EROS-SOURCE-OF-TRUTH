/**
 * Fix Caption Banks - Updates existing sheets with proper headers
 * Works with your current sheet structure
 */

/**
 * Fix headers for existing caption sheets
 * This function updates your existing sheets with the proper column headers
 */
function fixExistingCaptionBanks() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const ui = SpreadsheetApp.getUi();
    
    // Get all sheets
    const sheets = ss.getSheets();
    
    sheets.forEach(sheet => {
      const sheetName = sheet.getName();
      console.log('Processing sheet:', sheetName);
      
      // Skip non-caption sheets
      if (sheetName.includes('Week Plan') || 
          sheetName.includes('My Day') || 
          sheetName.includes('Brief') || 
          sheetName.includes('Settings') || 
          sheetName.includes('Activity Log') ||
          sheetName.includes('Alerts')) {
        console.log('Skipping system sheet:', sheetName);
        return;
      }
      
      // Check if this sheet has caption data
      const firstRow = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      const hasCaption = firstRow.some(cell => 
        String(cell).toLowerCase().includes('caption') || 
        String(cell).toLowerCase().includes('text')
      );
      
      if (!hasCaption && sheet.getLastRow() < 2) {
        console.log('Skipping empty sheet:', sheetName);
        return;
      }
      
      // Detect the type of caption bank based on content
      const captionType = detectCaptionType(sheet);
      console.log('Detected type:', captionType, 'for sheet:', sheetName);
      
      if (captionType) {
        updateSheetHeaders(sheet, captionType);
        console.log('Updated headers for:', sheetName);
      }
    });
    
    ui.alert(
      '✅ Caption Banks Fixed',
      'All caption bank headers have been updated successfully.',
      ui.ButtonSet.OK
    );
    
  } catch(e) {
    console.error('Error fixing caption banks:', e);
    SpreadsheetApp.getUi().alert('Error', 'Failed to fix caption banks: ' + e.message, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * Detect caption type based on sheet content
 */
function detectCaptionType(sheet) {
  try {
    // Get sample data from the sheet
    const lastRow = Math.min(sheet.getLastRow(), 20);
    if (lastRow < 2) return null;
    
    const data = sheet.getRange(1, 1, lastRow, sheet.getLastColumn()).getValues();
    
    // Check headers and content for keywords
    const allText = data.flat().join(' ').toLowerCase();
    
    // Check sheet name first for hints
    const sheetName = sheet.getName().toLowerCase();
    
    if (sheetName.includes('ppv')) return 'PPV';
    if (sheetName.includes('renew')) return 'RENEW';
    if (sheetName.includes('tip')) return 'TIP';
    if (sheetName.includes('bump') || sheetName.includes('follow')) return 'BUMPS';
    if (sheetName.includes('link')) return 'LINK';
    if (sheetName.includes('drip') || sheetName.includes('wall')) return 'DRIP';
    
    // Detect based on content patterns
    if (allText.includes('renew') || allText.includes('rebill') || allText.includes('subscription')) {
      return 'RENEW';
    } else if (allText.includes('tip') && (allText.includes('game') || allText.includes('spin') || allText.includes('goal'))) {
      return 'TIP';
    } else if (allText.includes('bump') || allText.includes('follow') || allText.includes('last chance') || allText.includes('still available')) {
      return 'BUMPS';
    } else if (allText.includes('link') || allText.includes('bundle') || allText.includes('pinned')) {
      return 'LINK';
    } else if (allText.includes('wall') || allText.includes('drip') || allText.includes('feed')) {
      return 'DRIP';
    } else if (allText.includes('ppv') || allText.includes('price') || allText.includes('massmessage')) {
      return 'PPV';
    }
    
    // Default to PPV if has caption content
    const hasCaption = data[0].some(cell => 
      String(cell).toLowerCase().includes('caption') || 
      String(cell).toLowerCase().includes('text')
    );
    
    return hasCaption ? 'PPV' : null;
    
  } catch(e) {
    console.error('Error detecting caption type:', e);
    return null;
  }
}

/**
 * Update sheet headers based on caption type
 */
function updateSheetHeaders(sheet, type) {
  const columnConfigs = {
    PPV: [
      {header: 'Caption ID', width: 80},
      {header: 'Caption Text', width: 400},
      {header: 'Price', width: 80},
      {header: 'Category', width: 120},
      {header: 'Performance Score', width: 120},
      {header: 'Avg Revenue', width: 100},
      {header: 'Open Rate %', width: 100},
      {header: 'Buy Rate %', width: 100},
      {header: 'Last Used Days', width: 120},
      {header: 'Best Time', width: 100},
      {header: 'Best Day', width: 100},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    BUMPS: [
      {header: 'Bump ID', width: 80},
      {header: 'Follow-up Text', width: 400},
      {header: 'Original PPV', width: 120},
      {header: 'Bump Type', width: 100},
      {header: 'Hours After', width: 100},
      {header: 'Discount %', width: 100},
      {header: 'Conversion Lift %', width: 120},
      {header: 'Avg Revenue', width: 100},
      {header: 'Response Rate %', width: 120},
      {header: 'Best Timing', width: 100},
      {header: 'Page Type', width: 100},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    RENEW: [
      {header: 'Campaign ID', width: 80},
      {header: 'Renewal Message', width: 400},
      {header: 'Campaign Type', width: 120},
      {header: 'Days Before Expiry', width: 130},
      {header: 'Incentive', width: 150},
      {header: 'Renewal Rate %', width: 120},
      {header: 'LTV Impact $', width: 110},
      {header: 'Best Time', width: 100},
      {header: 'Urgency Level', width: 110},
      {header: 'Page Type', width: 100},
      {header: 'Last Used Days', width: 120},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    TIP: [
      {header: 'Campaign ID', width: 80},
      {header: 'Tip Request Text', width: 400},
      {header: 'Campaign Style', width: 120},
      {header: 'Tip Amount $', width: 100},
      {header: 'Reward Offered', width: 150},
      {header: 'Tip Rate %', width: 100},
      {header: 'Avg Tip $', width: 100},
      {header: 'Total Revenue $', width: 120},
      {header: 'Best Day/Time', width: 120},
      {header: 'Engagement Score', width: 130},
      {header: 'Last Used Days', width: 120},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    DRIP: [
      {header: 'Post ID', width: 80},
      {header: 'Post Caption', width: 400},
      {header: 'Content Type', width: 120},
      {header: 'Time Slot', width: 100},
      {header: 'Style', width: 100},
      {header: 'Engagement %', width: 110},
      {header: 'DM Rate %', width: 100},
      {header: 'Best Time', width: 100},
      {header: 'Media Type', width: 100},
      {header: 'Mood Match', width: 100},
      {header: 'Last Used Days', width: 120},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    LINK: [
      {header: 'Drop ID', width: 80},
      {header: 'Link Drop Text', width: 400},
      {header: 'Drop Type', width: 120},
      {header: 'Target', width: 150},
      {header: 'CTA', width: 120},
      {header: 'Click Rate %', width: 110},
      {header: 'Conv Rate %', width: 110},
      {header: 'Revenue $', width: 100},
      {header: 'Best Place', width: 110},
      {header: 'Urgency', width: 100},
      {header: 'Last Used Days', width: 120},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ]
  };
  
  const columns = columnConfigs[type] || columnConfigs.PPV;
  
  // Clear row 1 and add new headers
  const headerRange = sheet.getRange(1, 1, 1, columns.length);
  headerRange.clear();
  
  // Set headers
  const headers = columns.map(col => col.header);
  headerRange.setValues([headers]);
  
  // Format headers
  headerRange.setBackground('#1e3a8a');
  headerRange.setFontColor('#ffffff');
  headerRange.setFontWeight('bold');
  headerRange.setHorizontalAlignment('center');
  headerRange.setWrap(true);
  
  // Set column widths
  columns.forEach((col, index) => {
    sheet.setColumnWidth(index + 1, col.width);
  });
  
  // Add data validation for Status column
  const statusCol = headers.indexOf('Status') + 1;
  if (statusCol > 0) {
    const lastRow = Math.max(sheet.getLastRow(), 100);
    const statusRange = sheet.getRange(2, statusCol, lastRow - 1, 1);
    const validation = SpreadsheetApp.newDataValidation()
      .requireValueInList(['🟢 Active', '🟡 Cooldown', '🔴 Retired', '⚪ Draft'], true)
      .build();
    statusRange.setDataValidation(validation);
  }
  
  // Freeze header row
  sheet.setFrozenRows(1);
  
  // Add alternating row colors
  if (sheet.getLastRow() > 1) {
    try {
      // Remove any existing banding first
      const bandings = sheet.getBandings();
      bandings.forEach(banding => banding.remove());
      
      // Now apply new banding
      const dataRange = sheet.getRange(2, 1, sheet.getLastRow() - 1, columns.length);
      dataRange.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
    } catch(e) {
      console.log('Could not apply banding:', e);
      // Continue without banding if it fails
    }
  }
  
  // Add formatting for percentage and currency columns
  headers.forEach((header, index) => {
    const col = index + 1;
    const lastRow = Math.max(sheet.getLastRow(), 100);
    
    if (lastRow > 1) {
      const range = sheet.getRange(2, col, lastRow - 1, 1);
      
      // Format percentage columns
      if (header.includes('%')) {
        range.setNumberFormat('0.0%');
        
        // Simple conditional formatting - high values green, low values red
        try {
          const rules = [];
          
          // Green for high percentages
          rules.push(
            SpreadsheetApp.newConditionalFormatRule()
              .whenNumberGreaterThanOrEqualTo(0.7)
              .setBackground('#dcfce7')
              .setRanges([range])
              .build()
          );
          
          // Yellow for medium
          rules.push(
            SpreadsheetApp.newConditionalFormatRule()
              .whenNumberBetween(0.3, 0.7)
              .setBackground('#fef3c7')
              .setRanges([range])
              .build()
          );
          
          // Red for low
          rules.push(
            SpreadsheetApp.newConditionalFormatRule()
              .whenNumberLessThan(0.3)
              .setBackground('#fee2e2')
              .setRanges([range])
              .build()
          );
          
          const existingRules = sheet.getConditionalFormatRules();
          sheet.setConditionalFormatRules(existingRules.concat(rules));
        } catch(e) {
          console.log('Could not add conditional formatting:', e);
        }
      }
      
      // Format currency columns
      if (header.includes('$') || header.includes('Revenue') || header.includes('Tip')) {
        range.setNumberFormat('$#,##0.00');
      }
      
      // Format number columns
      if (header.includes('Days') || header.includes('Hours') || header.includes('Score')) {
        range.setNumberFormat('0');
      }
    }
  });
}

/**
 * Quick function to just update the first visible sheet for testing
 */
function testFixFirstSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();
  
  console.log('Testing on sheet:', sheet.getName());
  
  // Detect type
  const type = detectCaptionType(sheet);
  console.log('Detected type:', type);
  
  if (type) {
    updateSheetHeaders(sheet, type);
    SpreadsheetApp.getUi().alert('Headers updated for: ' + sheet.getName() + ' (Type: ' + type + ')');
  } else {
    SpreadsheetApp.getUi().alert('Could not detect caption type for: ' + sheet.getName());
  }
}