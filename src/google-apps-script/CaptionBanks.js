/**
 * Caption Banks Setup and Management
 * Creates and maintains caption bank tabs with clear, user-friendly headers
 */

// Caption Bank Tab Configuration
const CAPTION_BANKS_CONFIG = {
  TABS: {
    PPV: '💰 PPV Captions',
    BUMPS: '🔄 Follow-up Bumps',
    RENEW: '💎 Renewal Campaigns', 
    TIP: '💸 Tip Campaigns',
    DRIP: '💧 DripSet (Wall/MM)',
    LINK: '🔗 Link Drops'
  },
  
  // Column definitions for each caption bank type
  COLUMNS: {
    PPV: [
      {header: 'ID', width: 60, note: 'Unique caption identifier'},
      {header: 'Caption Text', width: 400, note: 'The actual message to send'},
      {header: 'Category', width: 100, note: 'Type: Solo, B/G, Fetish, etc.'},
      {header: 'Price Range', width: 100, note: 'Recommended price: $10-50'},
      {header: 'Last Used', width: 100, note: 'Days since last sent'},
      {header: 'Performance Score', width: 120, note: '1-10 rating based on revenue'},
      {header: 'Avg Revenue', width: 100, note: 'Average earnings per send'},
      {header: 'Open Rate', width: 90, note: 'Percentage who opened'},
      {header: 'Buy Rate', width: 90, note: 'Percentage who purchased'},
      {header: 'Best Time', width: 100, note: 'Optimal send time'},
      {header: 'Best Day', width: 100, note: 'Best performing day'},
      {header: 'Status', width: 80, note: '🟢Ready 🟡Cooldown 🔴Overused'},
      {header: 'Notes', width: 150, note: 'Special instructions or warnings'}
    ],
    
    BUMPS: [
      {header: 'ID', width: 60, note: 'Unique caption identifier'},
      {header: 'Follow-up Text', width: 400, note: 'The bump message'},
      {header: 'Bump Type', width: 100, note: 'Soft/Medium/Hard sell'},
      {header: 'Hours After PPV', width: 100, note: 'When to send: 2h, 6h, 24h'},
      {header: 'Discount Offered', width: 100, note: 'Price reduction if any'},
      {header: 'Last Used', width: 100, note: 'Days since last sent'},
      {header: 'Conversion Lift', width: 120, note: '% increase in sales'},
      {header: 'Avg Revenue', width: 100, note: 'Additional revenue generated'},
      {header: 'Response Rate', width: 90, note: '% who respond'},
      {header: 'Best Timing', width: 100, note: 'Optimal hours after PPV'},
      {header: 'Page Type', width: 100, note: 'Free/VIP/Both'},
      {header: 'Status', width: 80, note: '🟢Ready 🟡Cooldown 🔴Overused'},
      {header: 'Notes', width: 150, note: 'Usage tips'}
    ],
    
    RENEW: [
      {header: 'ID', width: 60, note: 'Unique caption identifier'},
      {header: 'Renewal Message', width: 400, note: 'The campaign text'},
      {header: 'Campaign Type', width: 120, note: 'Expiring/Expired/Win-back'},
      {header: 'Days Before Expiry', width: 120, note: 'When to send: -7, -3, -1, 0'},
      {header: 'Incentive Offered', width: 150, note: 'Free PPV, discount, exclusive content'},
      {header: 'Last Used', width: 100, note: 'Days since last sent'},
      {header: 'Renewal Rate', width: 100, note: '% who renewed'},
      {header: 'Avg LTV Impact', width: 120, note: 'Lifetime value increase'},
      {header: 'Best Send Time', width: 100, note: 'Optimal time of day'},
      {header: 'Urgency Level', width: 100, note: 'Low/Medium/High/FOMO'},
      {header: 'Page Type', width: 100, note: 'Free/VIP/Both'},
      {header: 'Status', width: 80, note: '🟢Ready 🟡Cooldown 🔴Overused'},
      {header: 'Notes', width: 150, note: 'Special conditions'}
    ],
    
    TIP: [
      {header: 'ID', width: 60, note: 'Unique caption identifier'},
      {header: 'Tip Request', width: 400, note: 'The tip campaign message'},
      {header: 'Campaign Style', width: 120, note: 'Game/Goal/Appreciation/Challenge'},
      {header: 'Tip Amount', width: 100, note: 'Suggested tip: $5, $10, etc'},
      {header: 'Reward Offered', width: 150, note: 'What they get for tipping'},
      {header: 'Last Used', width: 100, note: 'Days since last sent'},
      {header: 'Tip Rate', width: 100, note: '% who tipped'},
      {header: 'Avg Tip Size', width: 100, note: 'Average amount received'},
      {header: 'Total Revenue', width: 100, note: 'Total earned from this caption'},
      {header: 'Best Day/Time', width: 120, note: 'When it works best'},
      {header: 'Engagement Score', width: 100, note: 'Overall interaction rate'},
      {header: 'Status', width: 80, note: '🟢Ready 🟡Cooldown 🔴Overused'},
      {header: 'Notes', width: 150, note: 'Tips for success'}
    ],
    
    DRIP: [
      {header: 'ID', width: 60, note: 'Unique caption identifier'},
      {header: 'Post Caption', width: 400, note: 'Wall post or MM text'},
      {header: 'Content Type', width: 100, note: 'Wall Post/Mass Message'},
      {header: 'Time Slot', width: 100, note: 'Morning/Afternoon/Evening/Late'},
      {header: 'Engagement Style', width: 120, note: 'Casual/Flirty/Direct/Teasing'},
      {header: 'Last Used', width: 100, note: 'Days since last sent'},
      {header: 'Engagement Rate', width: 100, note: '% who interact'},
      {header: 'DM Conversion', width: 100, note: '% who message after'},
      {header: 'Best Post Time', width: 100, note: 'Optimal time slot'},
      {header: 'Media Type', width: 100, note: 'Photo/Video/Text only'},
      {header: 'Mood Match', width: 100, note: 'Energy level for time of day'},
      {header: 'Status', width: 80, note: '🟢Ready 🟡Cooldown 🔴Overused'},
      {header: 'Notes', width: 150, note: 'Pairing suggestions'}
    ],
    
    LINK: [
      {header: 'ID', width: 60, note: 'Unique caption identifier'},
      {header: 'Link Drop Text', width: 400, note: 'The link promotion message'},
      {header: 'Drop Type', width: 120, note: 'Bundle/Pinned/Campaign/External'},
      {header: 'Target Content', width: 150, note: 'What the link points to'},
      {header: 'Call to Action', width: 150, note: 'The specific action requested'},
      {header: 'Last Used', width: 100, note: 'Days since last sent'},
      {header: 'Click Rate', width: 100, note: '% who clicked'},
      {header: 'Conversion Rate', width: 100, note: '% who completed action'},
      {header: 'Revenue Impact', width: 100, note: 'Money generated'},
      {header: 'Best Placement', width: 120, note: 'When in schedule to drop'},
      {header: 'Urgency Type', width: 100, note: 'None/Soft/Hard/Limited'},
      {header: 'Status', width: 80, note: '🟢Ready 🟡Cooldown 🔴Overused'},
      {header: 'Notes', width: 150, note: 'Usage guidelines'}
    ]
  }
};

/**
 * Initialize all caption bank tabs with proper headers
 */
function initializeCaptionBanks() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Create each caption bank tab
    Object.entries(CAPTION_BANKS_CONFIG.TABS).forEach(([type, tabName]) => {
      let sheet = ss.getSheetByName(tabName);
      
      // Create sheet if it doesn't exist
      if (!sheet) {
        sheet = ss.insertSheet(tabName);
        console.log('Created tab:', tabName);
      }
      
      // Set up the headers
      setupCaptionBankHeaders(sheet, type);
      
      // Load initial data
      loadCaptionBankData(sheet, type);
    });
    
    SpreadsheetApp.getUi().alert(
      '✅ Caption Banks Initialized',
      'All caption bank tabs have been created with proper headers.',
      SpreadsheetApp.getUi().ButtonSet.OK
    );
    
  } catch(e) {
    console.error('Error initializing caption banks:', e);
    SpreadsheetApp.getUi().alert('Error', 'Failed to initialize caption banks: ' + e.message, SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * Set up headers for a specific caption bank sheet
 */
function setupCaptionBankHeaders(sheet, type) {
  const columns = CAPTION_BANKS_CONFIG.COLUMNS[type];
  
  if (!columns) {
    console.error('No column configuration for type:', type);
    return;
  }
  
  // Clear existing content
  sheet.clear();
  
  // Set up title row
  sheet.getRange(1, 1).setValue(CAPTION_BANKS_CONFIG.TABS[type] + ' Bank').setFontSize(16).setFontWeight('bold');
  sheet.getRange(1, 1, 1, columns.length).merge().setHorizontalAlignment('center').setBackground('#1e40af').setFontColor('white');
  
  // Add description row
  const descriptions = {
    PPV: '💰 Pay-per-view messages - Your money makers! These drive the bulk of revenue.',
    BUMPS: '🔄 Follow-up messages to non-buyers - Second chance to convert!',
    RENEW: '💎 Keep subscribers active - Retention is cheaper than acquisition!',
    TIP: '💸 Interactive tip campaigns - Build engagement and extra revenue!',
    DRIP: '💧 Free content for the feed - Keep pages alive and engaging!',
    LINK: '🔗 Drive traffic to specific content - Strategic promotion tools!'
  };
  
  sheet.getRange(2, 1).setValue(descriptions[type]).setFontStyle('italic');
  sheet.getRange(2, 1, 1, columns.length).merge().setBackground('#f3f4f6');
  
  // Set up column headers (row 3)
  const headers = columns.map(col => col.header);
  sheet.getRange(3, 1, 1, headers.length).setValues([headers]);
  sheet.getRange(3, 1, 1, headers.length).setBackground('#374151').setFontColor('white').setFontWeight('bold');
  
  // Set column widths
  columns.forEach((col, index) => {
    sheet.setColumnWidth(index + 1, col.width);
    // Add notes to headers
    if (col.note) {
      sheet.getRange(3, index + 1).setNote(col.note);
    }
  });
  
  // Add data validation for Status column
  const statusCol = columns.findIndex(col => col.header === 'Status') + 1;
  if (statusCol > 0) {
    const validation = SpreadsheetApp.newDataValidation()
      .requireValueInList(['🟢 Ready', '🟡 Cooldown', '🔴 Overused'], true)
      .build();
    sheet.getRange(4, statusCol, 1000, 1).setDataValidation(validation);
  }
  
  // Freeze header rows
  sheet.setFrozenRows(3);
  
  // Add alternating row colors
  const range = sheet.getRange(4, 1, 1000, columns.length);
  range.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);
  
  // Add conditional formatting for performance columns
  addConditionalFormatting(sheet, type);
}

/**
 * Add conditional formatting to highlight good/bad performance
 */
function addConditionalFormatting(sheet, type) {
  const columns = CAPTION_BANKS_CONFIG.COLUMNS[type];
  
  // Find performance-related columns
  const performanceColumns = {
    'Performance Score': {min: 3, mid: 5, max: 8},
    'Open Rate': {min: 0.1, mid: 0.2, max: 0.3},
    'Buy Rate': {min: 0.02, mid: 0.05, max: 0.08},
    'Conversion Lift': {min: 5, mid: 10, max: 20},
    'Renewal Rate': {min: 0.3, mid: 0.5, max: 0.7},
    'Tip Rate': {min: 0.05, mid: 0.1, max: 0.15},
    'Click Rate': {min: 0.1, mid: 0.2, max: 0.3},
    'Engagement Rate': {min: 0.1, mid: 0.2, max: 0.3}
  };
  
  columns.forEach((col, index) => {
    if (performanceColumns[col.header]) {
      const colLetter = String.fromCharCode(65 + index);
      const range = sheet.getRange(colLetter + '4:' + colLetter);
      
      const rules = [
        // Red for poor performance
        SpreadsheetApp.newConditionalFormatRule()
          .whenNumberLessThan(performanceColumns[col.header].min)
          .setBackground('#fee2e2')
          .setRanges([range])
          .build(),
        
        // Yellow for average
        SpreadsheetApp.newConditionalFormatRule()
          .whenNumberBetween(performanceColumns[col.header].min, performanceColumns[col.header].mid)
          .setBackground('#fef3c7')
          .setRanges([range])
          .build(),
        
        // Green for good performance
        SpreadsheetApp.newConditionalFormatRule()
          .whenNumberGreaterThan(performanceColumns[col.header].max)
          .setBackground('#dcfce7')
          .setRanges([range])
          .build()
      ];
      
      sheet.setConditionalFormatRules(sheet.getConditionalFormatRules().concat(rules));
    }
  });
}

/**
 * Load caption bank data from BigQuery
 */
function loadCaptionBankData(sheet, type) {
  try {
    // Map type to BigQuery table
    const tableMap = {
      PPV: 'mart.caption_bank_ppv_v1',
      BUMPS: 'mart.caption_bank_bumps_v1',
      RENEW: 'mart.caption_bank_renew_v1',
      TIP: 'mart.caption_bank_tip_v1',
      DRIP: 'mart.caption_bank_drip_v1',
      LINK: 'mart.caption_bank_link_v1'
    };
    
    const table = tableMap[type];
    if (!table) {
      console.log('No data table configured for:', type);
      return;
    }
    
    // Query to get caption bank data
    const sql = `
      SELECT *
      FROM \`${CONFIG.PROJECT_ID}.${table}\`
      ORDER BY performance_score DESC
      LIMIT 100
    `;
    
    try {
      const results = BigQuery.Jobs.query({
        query: sql,
        useLegacySql: false,
        maxResults: 100
      }, CONFIG.PROJECT_ID);
      
      if (results.rows && results.rows.length > 0) {
        // Convert BigQuery results to sheet format
        const data = results.rows.map(row => {
          return CAPTION_BANKS_CONFIG.COLUMNS[type].map(col => {
            // Map column headers to data fields
            const fieldMap = {
              'ID': row.f[0] ? row.f[0].v : '',
              'Caption Text': row.f[1] ? row.f[1].v : '',
              'Follow-up Text': row.f[1] ? row.f[1].v : '',
              'Renewal Message': row.f[1] ? row.f[1].v : '',
              'Tip Request': row.f[1] ? row.f[1].v : '',
              'Post Caption': row.f[1] ? row.f[1].v : '',
              'Link Drop Text': row.f[1] ? row.f[1].v : '',
              // Add more field mappings as needed
            };
            
            return fieldMap[col.header] || '';
          });
        });
        
        // Write data to sheet starting from row 4
        if (data.length > 0) {
          sheet.getRange(4, 1, data.length, data[0].length).setValues(data);
        }
      }
    } catch(e) {
      console.log('Caption bank table not found, adding sample data for:', type);
      addSampleCaptionData(sheet, type);
    }
    
  } catch(e) {
    console.error('Error loading caption bank data:', e);
  }
}

/**
 * Add sample caption data for demonstration
 */
function addSampleCaptionData(sheet, type) {
  const sampleData = {
    PPV: [
      ['PPV-001', 'Just filmed something special for you... 🔥 Want to see what happens when I...', 'Solo', '$25-35', '5', '8.5', '$32.50', '28%', '7.2%', '9PM-11PM', 'Friday', '🟢 Ready', 'High performer'],
      ['PPV-002', 'You\'ve been such a good boy... ready for your reward? 😈', 'B/G', '$35-45', '12', '7.8', '$41.20', '25%', '6.5%', '10PM-12AM', 'Saturday', '🟢 Ready', 'Weekend winner'],
      ['PPV-003', 'OMG I can\'t believe I did this... swipe up if you dare 👀', 'Fetish', '$40-50', '8', '9.2', '$48.75', '32%', '8.9%', '8PM-10PM', 'Thursday', '🟡 Cooldown', 'Use sparingly']
    ],
    BUMPS: [
      ['BUMP-001', 'Hey babe, did you see what I sent earlier? It expires soon...', 'Soft', '2-4 hours', '20%', '3', '15%', '$12.50', '18%', '3 hours', 'Both', '🟢 Ready', 'Gentle reminder'],
      ['BUMP-002', 'Last chance! Only 30 mins left to unlock... don\'t miss out 🔥', 'Hard', '6-8 hours', '30%', '7', '22%', '$18.90', '24%', '6 hours', 'VIP', '🟢 Ready', 'Urgency works'],
      ['BUMP-003', 'Still available if you want it... just saying 😘', 'Medium', '24 hours', '15%', '10', '12%', '$8.75', '15%', '24 hours', 'Free', '🟡 Cooldown', 'Casual approach']
    ],
    RENEW: [
      ['REN-001', 'Your subscription expires in 3 days! Renew now for exclusive content 💎', 'Expiring', '-3 days', 'Exclusive PPV', '2', '45%', '$85.00', '8PM', 'High', 'Both', '🟢 Ready', 'Strong converter'],
      ['REN-002', 'Miss me already? Come back for 50% off this month only 💕', 'Win-back', '+7 days', '50% discount', '15', '28%', '$42.50', '7PM', 'Medium', 'Free', '🟢 Ready', 'Win-back special'],
      ['REN-003', 'Don\'t leave me! Renew today and get a special surprise 🎁', 'Expiring', '-1 day', 'Mystery gift', '5', '52%', '$95.00', '9PM', 'FOMO', 'VIP', '🟡 Cooldown', 'Last day push']
    ],
    TIP: [
      ['TIP-001', 'Tip $5 if you think I\'m cute, $10 if you want to see more 😊', 'Appreciation', '$5-10', 'Thank you video', '8', '12%', '$7.50', '$890', 'Weekend evenings', '8.5', '🟢 Ready', 'Simple & effective'],
      ['TIP-002', 'Let\'s play a game! Tip to spin the wheel and win prizes 🎰', 'Game', '$10-20', 'Prize wheel spin', '4', '18%', '$15.25', '$1,250', 'Friday night', '9.2', '🟢 Ready', 'Interactive fun'],
      ['TIP-003', 'Help me reach my goal! $500 for new lingerie haul 👙', 'Goal', '$20+', 'Lingerie photos', '12', '8%', '$25.00', '$650', 'Start of month', '7.0', '🟡 Cooldown', 'Goal-based']
    ],
    DRIP: [
      ['DRIP-001', 'Good morning babies! Who\'s ready to play today? 😘', 'Mass Message', 'Morning', 'Flirty', '3', '35%', '12%', '7AM-9AM', 'Photo', 'High energy', '🟢 Ready', 'Morning starter'],
      ['DRIP-002', 'Feeling so naughty right now... check your DMs 😈', 'Wall Post', 'Evening', 'Direct', '7', '42%', '18%', '9PM-11PM', 'Video', 'Sultry', '🟢 Ready', 'Evening heat'],
      ['DRIP-003', 'Bored at home... entertain me? 🥺', 'Mass Message', 'Afternoon', 'Casual', '10', '28%', '8%', '2PM-4PM', 'Photo', 'Relaxed', '🟡 Cooldown', 'Afternoon engagement']
    ],
    LINK: [
      ['LINK-001', 'New bundle dropped! 50 pics + 10 vids for one low price 👇', 'Bundle', 'Content bundle', 'Click to unlock', '5', '22%', '35%', '$125', 'After PPV', 'Limited', '🟢 Ready', 'Bundle promo'],
      ['LINK-002', 'Check out my pinned post for something special... 📌', 'Pinned', 'Pinned content', 'View pinned', '8', '18%', '28%', '$75', 'Morning', 'Soft', '🟢 Ready', 'Pinned driver'],
      ['LINK-003', 'Follow my backup account! Link in bio 🔗', 'External', 'Backup account', 'Follow now', '12', '15%', '45%', '$0', 'Anytime', 'None', '🟡 Cooldown', 'Cross-platform']
    ]
  };
  
  const data = sampleData[type];
  if (data && data.length > 0) {
    sheet.getRange(4, 1, data.length, data[0].length).setValues(data);
  }
}

/**
 * Refresh all caption bank data
 */
function refreshAllCaptionBanks() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    Object.entries(CAPTION_BANKS_CONFIG.TABS).forEach(([type, tabName]) => {
      const sheet = ss.getSheetByName(tabName);
      if (sheet) {
        loadCaptionBankData(sheet, type);
        console.log('Refreshed:', tabName);
      }
    });
    
    SpreadsheetApp.getUi().alert('✅ Caption banks refreshed with latest data');
    
  } catch(e) {
    handleError(e, 'refreshAllCaptionBanks');
  }
}

/**
 * Search caption banks for specific criteria
 */
function searchCaptionBanks() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.prompt(
    'Search Caption Banks',
    'Enter search term (searches all caption text):',
    ui.ButtonSet.OK_CANCEL
  );
  
  if (response.getSelectedButton() !== ui.Button.OK) return;
  
  const searchTerm = response.getResponseText().toLowerCase();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const results = [];
  
  Object.entries(CAPTION_BANKS_CONFIG.TABS).forEach(([type, tabName]) => {
    const sheet = ss.getSheetByName(tabName);
    if (sheet) {
      const data = sheet.getDataRange().getValues();
      
      for (let i = 3; i < data.length; i++) {
        const row = data[i];
        const captionText = (row[1] || '').toLowerCase();
        
        if (captionText.includes(searchTerm)) {
          results.push({
            type: type,
            id: row[0],
            caption: row[1].substring(0, 100) + '...',
            performance: row[6] || row[5] || 'N/A'
          });
        }
      }
    }
  });
  
  if (results.length === 0) {
    ui.alert('No results found for: ' + searchTerm);
  } else {
    let message = 'Found ' + results.length + ' matches:\n\n';
    results.slice(0, 10).forEach(r => {
      message += r.type + ' | ' + r.id + ' | ' + r.caption + '\n';
    });
    
    if (results.length > 10) {
      message += '\n... and ' + (results.length - 10) + ' more results';
    }
    
    ui.alert('Search Results', message, ui.ButtonSet.OK);
  }
}