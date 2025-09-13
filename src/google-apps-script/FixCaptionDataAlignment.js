/**
 * Fix Caption Data Alignment - Properly align existing data with correct headers
 */

function fixCaptionDataAlignment() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ui = SpreadsheetApp.getUi();
  
  // Process each caption bank sheet
  ['BUMPS', 'TIP Captions', 'Renew', 'PPVS'].forEach(sheetName => {
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) {
      console.log('Sheet not found:', sheetName);
      return;
    }
    
    console.log('Processing:', sheetName);
    
    // Get all data including headers
    const allData = sheet.getDataRange().getValues();
    if (allData.length < 2) return;
    
    // Detect sheet type and realign
    if (sheetName.includes('BUMP')) {
      fixBumpsAlignment(sheet, allData);
    } else if (sheetName.includes('TIP')) {
      fixTipAlignment(sheet, allData);
    } else if (sheetName.includes('Renew')) {
      fixRenewAlignment(sheet, allData);
    } else if (sheetName.includes('PPV')) {
      fixPPVAlignment(sheet, allData);
    }
  });
  
  ui.alert('✅ Data Alignment Fixed', 'All caption bank data has been properly aligned with headers.', ui.ButtonSet.OK);
}

function fixBumpsAlignment(sheet, data) {
  // Clear sheet
  sheet.clear();
  
  // Set proper headers
  const headers = [
    'Bump ID',
    'Follow-up Text', 
    'Original PPV',
    'Bump Type',
    'Hours After',
    'Discount %',
    'Conversion Lift %',
    'Avg Revenue',
    'Response Rate %',
    'Best Timing',
    'Page Type',
    'Status'
  ];
  
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  formatHeaders(sheet, headers.length);
  
  // Process data rows - skip header
  const newData = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (!row[0] && !row[1]) continue;
    
    // The actual structure seems to be:
    // Col A: Number ID
    // Col B: Caption text
    // Col C: PPV reference or "0"
    // Col D: Type like "Follow Up MassMessage Bump"
    
    newData.push([
      row[0] || i,                                    // Bump ID
      row[1] || '',                                   // Follow-up Text (main caption)
      row[2] || '',                                   // Original PPV
      row[3] || 'Follow Up',                          // Bump Type
      '',                                              // Hours After
      '',                                              // Discount %
      '',                                              // Conversion Lift %
      '',                                              // Avg Revenue
      '',                                              // Response Rate %
      '',                                              // Best Timing
      '',                                              // Page Type
      '🟢 Active'                                      // Status
    ]);
  }
  
  if (newData.length > 0) {
    sheet.getRange(2, 1, newData.length, headers.length).setValues(newData);
  }
  
  // Set column widths
  sheet.setColumnWidth(1, 80);   // ID
  sheet.setColumnWidth(2, 500);  // Caption text
  sheet.setColumnWidth(3, 100);  // Original PPV
  sheet.setColumnWidth(4, 150);  // Type
  
  sheet.setFrozenRows(1);
}

function fixTipAlignment(sheet, data) {
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
  formatHeaders(sheet, headers.length);
  
  // Process data rows
  const newData = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (!row[0] && !row[1]) continue;
    
    // Current structure seems to be:
    // Col A: ID number
    // Col B: Dollar amount like "$1,097.00"
    // Col C: Caption text
    // Col D: Campaign style
    // Col E: Tip amount
    // Col F: Reward
    
    // Extract the actual caption text (might be in col C or col B)
    let captionText = '';
    let tipAmount = '';
    
    // If column B has dollar amount, caption is likely in column C
    if (String(row[1]).includes('$')) {
      tipAmount = row[1];
      captionText = row[2] || '';
    } else {
      // Otherwise caption might be in column B
      captionText = row[1] || row[2] || '';
      tipAmount = row[4] || '$0';
    }
    
    newData.push([
      i,                                               // Campaign ID
      captionText,                                     // Tip Request Text
      tipAmount || '$0',                              // Tip Amount
      row[3] || 'Tip campaign',                       // Campaign Style
      row[5] || 'Tip rewards',                        // Reward Offered
      '',                                              // Tip Rate %
      '',                                              // Avg Tip
      row[1] || '',                                   // Total Revenue (if dollar amount in B)
      '',                                              // Best Day/Time
      '',                                              // Engagement Score
      '🟢 Active'                                      // Status
    ]);
  }
  
  if (newData.length > 0) {
    sheet.getRange(2, 1, newData.length, headers.length).setValues(newData);
  }
  
  // Set column widths
  sheet.setColumnWidth(1, 100);  // ID
  sheet.setColumnWidth(2, 500);  // Caption text
  sheet.setColumnWidth(3, 120);  // Amount
  sheet.setColumnWidth(4, 150);  // Style
  
  sheet.setFrozenRows(1);
}

function fixRenewAlignment(sheet, data) {
  // Clear sheet
  sheet.clear();
  
  // Set proper headers
  const headers = [
    'Campaign ID',
    'Renewal Message',
    'Campaign Type',
    'Days Before Expiry',
    'Incentive',
    'Renewal Rate %',
    'LTV Impact $',
    'Best Time',
    'Urgency Level',
    'Page Type',
    'Status'
  ];
  
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  formatHeaders(sheet, headers.length);
  
  // Process data rows
  const newData = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (!row[0] && !row[1]) continue;
    
    // Current structure:
    // Col A: ID number
    // Col B: Renewal message text
    // Col C: Days or "0"
    // Col D: Type like "Renew campaign captions"
    
    newData.push([
      row[0] || i,                                     // Campaign ID
      row[1] || '',                                    // Renewal Message
      row[3] || 'Renewal',                            // Campaign Type
      row[2] || '0',                                  // Days Before Expiry
      row[4] || '',                                    // Incentive
      '',                                              // Renewal Rate %
      '',                                              // LTV Impact $
      '',                                              // Best Time
      '',                                              // Urgency Level
      '',                                              // Page Type
      '🟢 Active'                                      // Status
    ]);
  }
  
  if (newData.length > 0) {
    sheet.getRange(2, 1, newData.length, headers.length).setValues(newData);
  }
  
  // Set column widths
  sheet.setColumnWidth(1, 100);  // ID
  sheet.setColumnWidth(2, 500);  // Message
  sheet.setColumnWidth(3, 150);  // Type
  sheet.setColumnWidth(4, 130);  // Days
  
  sheet.setFrozenRows(1);
}

function fixPPVAlignment(sheet, data) {
  // Clear sheet
  sheet.clear();
  
  // Set proper headers
  const headers = [
    'Caption ID',
    'Caption Text',
    'Price',
    'Category',
    'Performance Score',
    'Avg Revenue',
    'Open Rate %',
    'Buy Rate %',
    'Last Used Days',
    'Best Time',
    'Best Day',
    'Status'
  ];
  
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  formatHeaders(sheet, headers.length);
  
  // Process data rows
  const newData = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (!row[0] && !row[1]) continue;
    
    // Current structure seems to be:
    // Col A: Caption text
    // Col B: Price
    // Col C: Category
    
    newData.push([
      i,                                               // Caption ID (auto-number)
      row[0] || '',                                   // Caption Text
      row[1] || '',                                   // Price
      row[2] || 'MassMessage PPV',                    // Category
      '',                                              // Performance Score
      '',                                              // Avg Revenue
      '',                                              // Open Rate %
      '',                                              // Buy Rate %
      '',                                              // Last Used Days
      '',                                              // Best Time
      '',                                              // Best Day
      '🟢 Active'                                      // Status
    ]);
  }
  
  if (newData.length > 0) {
    sheet.getRange(2, 1, newData.length, headers.length).setValues(newData);
  }
  
  // Set column widths
  sheet.setColumnWidth(1, 80);   // ID
  sheet.setColumnWidth(2, 500);  // Caption
  sheet.setColumnWidth(3, 80);   // Price
  sheet.setColumnWidth(4, 150);  // Category
  
  sheet.setFrozenRows(1);
}

function formatHeaders(sheet, numCols) {
  const headerRange = sheet.getRange(1, 1, 1, numCols);
  headerRange.setBackground('#1e3a8a');
  headerRange.setFontColor('#ffffff');
  headerRange.setFontWeight('bold');
  headerRange.setHorizontalAlignment('center');
  headerRange.setWrap(true);
  
  // Remove any existing banding
  try {
    const bandings = sheet.getBandings();
    bandings.forEach(b => b.remove());
    
    // Add new banding
    if (sheet.getLastRow() > 1) {
      const dataRange = sheet.getRange(2, 1, sheet.getLastRow() - 1, numCols);
      dataRange.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
    }
  } catch(e) {
    console.log('Could not apply banding:', e);
  }
}

/**
 * Test on active sheet only
 */
function testAlignActiveSheet() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const data = sheet.getDataRange().getValues();
  const name = sheet.getName();
  
  if (name.includes('BUMP')) {
    fixBumpsAlignment(sheet, data);
  } else if (name.includes('TIP')) {
    fixTipAlignment(sheet, data);
  } else if (name.includes('Renew')) {
    fixRenewAlignment(sheet, data);
  } else if (name.includes('PPV')) {
    fixPPVAlignment(sheet, data);
  }
  
  SpreadsheetApp.getUi().alert('✅ Sheet aligned: ' + name);
}