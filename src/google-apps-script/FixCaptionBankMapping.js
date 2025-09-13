/**
 * Fix Caption Bank Column Mapping
 * This properly maps the existing data to the correct column headers
 */

/**
 * Fix the column mapping for caption banks
 */
function fixCaptionBankMapping() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const ui = SpreadsheetApp.getUi();
    
    // Process each sheet
    const sheets = ss.getSheets();
    
    sheets.forEach(sheet => {
      const sheetName = sheet.getName();
      
      // Skip system sheets
      if (sheetName.includes('Week Plan') || 
          sheetName.includes('My Day') || 
          sheetName.includes('Brief') || 
          sheetName.includes('Settings') || 
          sheetName.includes('Activity Log') ||
          sheetName.includes('Alerts')) {
        console.log('Skipping system sheet:', sheetName);
        return;
      }
      
      // Get the data
      const lastRow = sheet.getLastRow();
      const lastCol = sheet.getLastColumn();
      
      if (lastRow < 2 || lastCol < 2) {
        console.log('Skipping empty sheet:', sheetName);
        return;
      }
      
      // Read all data
      const data = sheet.getRange(1, 1, lastRow, lastCol).getValues();
      
      // Detect the sheet type based on content
      const sheetType = detectSheetType(data, sheetName);
      console.log('Processing sheet:', sheetName, 'Type:', sheetType);
      
      if (sheetType) {
        reorganizeSheetData(sheet, data, sheetType);
      }
    });
    
    ui.alert('✅ Caption Banks Fixed', 'All caption bank columns have been properly mapped.', ui.ButtonSet.OK);
    
  } catch(e) {
    console.error('Error:', e);
    SpreadsheetApp.getUi().alert('Error', 'Failed to fix caption banks: ' + e.message, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * Detect sheet type based on data content
 */
function detectSheetType(data, sheetName) {
  const nameLC = sheetName.toLowerCase();
  
  // Check sheet name first
  if (nameLC.includes('ppv')) return 'PPV';
  if (nameLC.includes('renew')) return 'RENEW';
  if (nameLC.includes('tip')) return 'TIP';
  if (nameLC.includes('bump') || nameLC.includes('follow')) return 'BUMPS';
  
  // Check data content
  const allText = data.flat().join(' ').toLowerCase();
  
  if (allText.includes('renew campaign captions')) return 'RENEW';
  if (allText.includes('tip campaign captions')) return 'TIP';
  if (allText.includes('follow up massmessage bump')) return 'BUMPS';
  if (allText.includes('massmessage ppv')) return 'PPV';
  
  // Default to PPV if has price data
  return 'PPV';
}

/**
 * Reorganize sheet data with proper column mapping
 */
function reorganizeSheetData(sheet, existingData, sheetType) {
  // Clear the sheet
  sheet.clear();
  
  // Define proper column structure based on type
  const columnStructures = {
    PPV: [
      {header: 'Caption ID', width: 80},
      {header: 'Caption Text', width: 400},
      {header: 'Price', width: 80},
      {header: 'Category', width: 150},
      {header: 'Performance Score', width: 120},
      {header: 'Avg Revenue', width: 100},
      {header: 'Open Rate %', width: 100},
      {header: 'Buy Rate %', width: 100},
      {header: 'Last Used Days', width: 100},
      {header: 'Best Time', width: 100},
      {header: 'Best Day', width: 100},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    BUMPS: [
      {header: 'Bump ID', width: 80},
      {header: 'Follow-up Text', width: 400},
      {header: 'Original PPV ID', width: 100},
      {header: 'Bump Type', width: 150},
      {header: 'Hours After', width: 80},
      {header: 'Discount %', width: 80},
      {header: 'Conversion Lift %', width: 120},
      {header: 'Avg Revenue', width: 100},
      {header: 'Response Rate %', width: 100},
      {header: 'Best Timing', width: 100},
      {header: 'Page Type', width: 100},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    RENEW: [
      {header: 'Campaign ID', width: 80},
      {header: 'Renewal Message', width: 400},
      {header: 'Campaign Type', width: 150},
      {header: 'Days Before Expiry', width: 120},
      {header: 'Incentive', width: 150},
      {header: 'Renewal Rate %', width: 100},
      {header: 'LTV Impact $', width: 100},
      {header: 'Best Time', width: 100},
      {header: 'Urgency Level', width: 100},
      {header: 'Page Type', width: 100},
      {header: 'Last Used Days', width: 100},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ],
    
    TIP: [
      {header: 'Campaign ID', width: 80},
      {header: 'Tip Request Text', width: 400},
      {header: 'Campaign Style', width: 150},
      {header: 'Tip Amount', width: 100},
      {header: 'Reward Offered', width: 150},
      {header: 'Tip Rate %', width: 100},
      {header: 'Avg Tip $', width: 100},
      {header: 'Total Revenue $', width: 120},
      {header: 'Best Day/Time', width: 120},
      {header: 'Engagement Score', width: 100},
      {header: 'Last Used Days', width: 100},
      {header: 'Status', width: 100},
      {header: 'Notes', width: 200}
    ]
  };
  
  const columns = columnStructures[sheetType] || columnStructures.PPV;
  
  // Set up headers
  const headers = columns.map(col => col.header);
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  
  // Format headers
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setBackground('#1e3a8a');
  headerRange.setFontColor('#ffffff');
  headerRange.setFontWeight('bold');
  headerRange.setHorizontalAlignment('center');
  headerRange.setWrap(true);
  
  // Set column widths
  columns.forEach((col, index) => {
    sheet.setColumnWidth(index + 1, col.width);
  });
  
  // Map existing data to new structure
  if (existingData.length > 1) {
    const mappedData = mapDataToColumns(existingData.slice(1), sheetType);
    
    if (mappedData.length > 0) {
      sheet.getRange(2, 1, mappedData.length, mappedData[0].length).setValues(mappedData);
    }
  }
  
  // Freeze header row
  sheet.setFrozenRows(1);
  
  // Add alternating colors
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
}

/**
 * Map existing data to proper columns based on type
 */
function mapDataToColumns(data, sheetType) {
  const mappedRows = [];
  
  data.forEach((row, index) => {
    if (!row[0] && !row[1]) return; // Skip empty rows
    
    let mappedRow = [];
    
    switch(sheetType) {
      case 'PPV':
        // Current structure seems to be: caption_text, price, category
        mappedRow = [
          index + 1,                    // Caption ID (auto-number)
          row[0] || '',                 // Caption Text (was in col A)
          row[1] || '',                 // Price (was in col B)
          row[2] || 'MassMessage PPV',  // Category (was in col C)
          '',                            // Performance Score (empty)
          '',                            // Avg Revenue (empty)
          '',                            // Open Rate % (empty)
          '',                            // Buy Rate % (empty)
          '',                            // Last Used Days (empty)
          '',                            // Best Time (empty)
          '',                            // Best Day (empty)
          '🟢 Active',                   // Status (default)
          ''                             // Notes (empty)
        ];
        break;
        
      case 'BUMPS':
        // Current structure: ID in A, caption in B, PPV ref in C, type in D
        mappedRow = [
          row[0] || index + 1,          // Bump ID
          row[1] || '',                 // Follow-up Text
          row[2] || '',                 // Original PPV ID
          row[3] || 'Follow Up',        // Bump Type
          '',                            // Hours After (empty)
          '',                            // Discount % (empty)
          '',                            // Conversion Lift % (empty)
          '',                            // Avg Revenue (empty)
          '',                            // Response Rate % (empty)
          '',                            // Best Timing (empty)
          '',                            // Page Type (empty)
          '🟢 Active',                   // Status
          ''                             // Notes
        ];
        break;
        
      case 'RENEW':
        // Current structure: ID in A, caption in B, type in C, days in D
        mappedRow = [
          row[0] || index + 1,          // Campaign ID
          row[1] || '',                 // Renewal Message
          row[2] || 'Renew campaign',   // Campaign Type
          row[3] || '0',                // Days Before Expiry
          row[4] || '',                 // Incentive
          '',                            // Renewal Rate % (empty)
          '',                            // LTV Impact $ (empty)
          '',                            // Best Time (empty)
          '',                            // Urgency Level (empty)
          '',                            // Page Type (empty)
          '',                            // Last Used Days (empty)
          '🟢 Active',                   // Status
          ''                             // Notes
        ];
        break;
        
      case 'TIP':
        // Current structure: amount in A, caption in B, amount in C, type in D
        mappedRow = [
          index + 1,                     // Campaign ID
          row[1] || row[0] || '',        // Tip Request Text
          row[2] || 'Tip campaign',     // Campaign Style
          extractAmount(row[0]) || '$0', // Tip Amount
          row[3] || 'Tip campaign captions', // Reward Offered
          '',                            // Tip Rate % (empty)
          '',                            // Avg Tip $ (empty)
          extractAmount(row[0]) || '',  // Total Revenue $
          '',                            // Best Day/Time (empty)
          '',                            // Engagement Score (empty)
          '',                            // Last Used Days (empty)
          '🟢 Active',                   // Status
          ''                             // Notes
        ];
        break;
        
      default:
        // Default PPV mapping
        mappedRow = [
          index + 1,
          row[0] || '',
          row[1] || '',
          row[2] || '',
          '', '', '', '', '', '', '', '🟢 Active', ''
        ];
    }
    
    mappedRows.push(mappedRow);
  });
  
  return mappedRows;
}

/**
 * Extract dollar amount from text
 */
function extractAmount(text) {
  if (!text) return '';
  const match = String(text).match(/\$?([\d,]+(?:\.\d{2})?)/);
  return match ? '$' + match[1] : '';
}

/**
 * Test on active sheet only
 */
function testFixActiveSheet() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const data = sheet.getDataRange().getValues();
  const sheetType = detectSheetType(data, sheet.getName());
  
  console.log('Sheet:', sheet.getName());
  console.log('Detected type:', sheetType);
  console.log('First row:', data[0]);
  console.log('Second row:', data[1]);
  
  reorganizeSheetData(sheet, data, sheetType);
  
  SpreadsheetApp.getUi().alert('Sheet reorganized as type: ' + sheetType);
}