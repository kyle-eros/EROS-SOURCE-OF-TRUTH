/**
 * Fix TIP Tab Alignment - Separate caption text from dollar amounts
 */

function fixTipTabAlignment() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('TIP Captions') || ss.getSheetByName('💸 TIP Captions');
  
  if (!sheet) {
    SpreadsheetApp.getUi().alert('TIP Captions sheet not found');
    return;
  }
  
  // Get all data
  const data = sheet.getDataRange().getValues();
  
  // Clear sheet
  sheet.clear();
  
  // Set proper headers
  const headers = [
    'Campaign ID',
    'Tip Request Text',
    'Tip Amount',
    'Campaign Style',
    'Reward Offered',
    'Tip Rate %',
    'Avg Tip',
    'Total Revenue',
    'Best Day/Time',
    'Engagement Score',
    'Status'
  ];
  
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  
  // Format headers
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setBackground('#1e3a8a');
  headerRange.setFontColor('#ffffff');
  headerRange.setFontWeight('bold');
  headerRange.setHorizontalAlignment('center');
  headerRange.setWrap(true);
  
  // Process data rows
  const newData = [];
  
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (!row[0] && !row[1]) continue;
    
    // ACTUAL structure from your screenshot:
    // Col A: Campaign ID (1, 2, 3...)
    // Col B: Dollar amount ($1,097.00, $1,174.00...)
    // Col C: Caption text ("### THIS IS ONLY FOR...", "Tip if you like...")
    // Col D: Campaign Style ("Tip campaign captions")
    // Col E: Tip Amount ($0.00)
    // Col F: Reward ("Tip campaign captions")
    
    let campaignId = row[0] || i;
    let dollarAmount = String(row[1] || '');  // This is the dollar amount
    let captionText = String(row[2] || '');   // This is the actual caption text
    let tipAmount = '';
    
    // Clean up dollar amount (it might have formatting)
    if (dollarAmount.includes('$')) {
      tipAmount = dollarAmount.match(/\$[\d,]+\.?\d*/)?.[0] || dollarAmount;
    } else {
      tipAmount = row[4] || '$0';  // Check column E for tip amount
    }
    
    // Clean up the caption text - remove leading ### and spaces
    captionText = captionText.replace(/^#+\s*/, '').trim();
    
    // If caption is empty but we have text in another column, use it
    if (!captionText && row[3]) {
      captionText = String(row[3]);
    }
    
    // Try to extract tip amount from caption if we don't have it
    if ((!tipAmount || tipAmount === '$0') && captionText) {
      const tipMatch = captionText.match(/[Tt]ip\s*\$?([\d]+)/);
      if (tipMatch) {
        tipAmount = '$' + tipMatch[1];
      }
    }
    
    newData.push([
      campaignId,                                     // Campaign ID
      captionText,                                     // Tip Request Text (cleaned)
      tipAmount || '$0',                             // Tip Amount
      'Tip campaign',                                 // Campaign Style
      row[4] || 'Tip rewards',                       // Reward Offered
      '',                                             // Tip Rate %
      '',                                             // Avg Tip
      '',                                             // Total Revenue
      '',                                             // Best Day/Time
      '',                                             // Engagement Score
      '🟢 Active'                                     // Status
    ]);
  }
  
  // Write the reorganized data
  if (newData.length > 0) {
    sheet.getRange(2, 1, newData.length, headers.length).setValues(newData);
  }
  
  // Set column widths for better readability
  sheet.setColumnWidth(1, 100);  // Campaign ID
  sheet.setColumnWidth(2, 500);  // Tip Request Text (wide for captions)
  sheet.setColumnWidth(3, 100);  // Tip Amount
  sheet.setColumnWidth(4, 150);  // Campaign Style
  sheet.setColumnWidth(5, 200);  // Reward Offered
  sheet.setColumnWidth(6, 100);  // Tip Rate %
  sheet.setColumnWidth(7, 100);  // Avg Tip
  sheet.setColumnWidth(8, 120);  // Total Revenue
  sheet.setColumnWidth(9, 120);  // Best Day/Time
  sheet.setColumnWidth(10, 120); // Engagement Score
  sheet.setColumnWidth(11, 100); // Status
  
  // Freeze header row
  sheet.setFrozenRows(1);
  
  // Add alternating row colors
  try {
    const bandings = sheet.getBandings();
    bandings.forEach(b => b.remove());
    
    if (sheet.getLastRow() > 1) {
      const dataRange = sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length);
      dataRange.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
    }
  } catch(e) {
    console.log('Could not apply banding:', e);
  }
  
  // Format currency columns
  if (sheet.getLastRow() > 1) {
    // Tip Amount column
    sheet.getRange(2, 3, sheet.getLastRow() - 1, 1).setNumberFormat('$#,##0.00');
    // Avg Tip column
    sheet.getRange(2, 7, sheet.getLastRow() - 1, 1).setNumberFormat('$#,##0.00');
    // Total Revenue column
    sheet.getRange(2, 8, sheet.getLastRow() - 1, 1).setNumberFormat('$#,##0.00');
  }
  
  SpreadsheetApp.getUi().alert('✅ TIP Tab Fixed', 'Tip captions have been properly separated from amounts.', SpreadsheetApp.getUi().ButtonSet.OK);
}

/**
 * Quick test function for debugging
 */
function testTipDataParsing() {
  // Test examples from your data
  const testCases = [
    "$1,097.00 ### THIS IS ONLY FOR 🔥 1 FAN",
    "$1,174.00 ### FIRST FAN ONLY!!!! Tip $10 RIGHT NOW",
    "$1,625.00 Tip if you like 🥰",
    "Tip me $10 for a Christmas surprise",
    "$3,266.00 My bank account broke.... Just bought my lingerie"
  ];
  
  testCases.forEach(text => {
    const dollarMatch = text.match(/^\$[\d,]+\.?\d*/);
    
    if (dollarMatch) {
      const amount = dollarMatch[0];
      const caption = text.substring(dollarMatch[0].length).replace(/^[\s#]+/, '').trim();
      console.log('Amount:', amount, '| Caption:', caption);
    } else {
      console.log('No amount found, all caption:', text);
    }
  });
}