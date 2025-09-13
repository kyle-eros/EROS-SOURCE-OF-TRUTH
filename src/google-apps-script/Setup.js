/**
 * Automated Setup Script for EROS Scheduler Hub
 * This script runs all initialization functions in sequence
 */

function setupSchedulerHub() {
  console.log('🚀 Starting EROS Scheduler Hub Setup...');
  
  try {
    // Step 1: Initialize sheets and menu
    console.log('Step 1: Initializing sheets and menu...');
    onOpen();
    console.log('✅ Sheets and menu initialized');
    
    // Step 2: Authorize BigQuery
    console.log('Step 2: Authorizing BigQuery access...');
    authorizeOnce();
    console.log('✅ BigQuery authorized');
    
    // Step 3: Get the spreadsheet URL
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) {
      // Create a new spreadsheet if not bound
      const newSS = SpreadsheetApp.create('EROS Scheduler Hub v7.1');
      const ssId = newSS.getId();
      const ssUrl = newSS.getUrl();
      console.log('✅ Created new spreadsheet');
      console.log('📋 Spreadsheet ID:', ssId);
      console.log('🔗 Spreadsheet URL:', ssUrl);
      
      // Initialize the new spreadsheet
      SpreadsheetApp.setActiveSpreadsheet(newSS);
      onOpen();
    } else {
      const ssUrl = ss.getUrl();
      const ssId = ss.getId();
      console.log('✅ Using existing spreadsheet');
      console.log('📋 Spreadsheet ID:', ssId);
      console.log('🔗 Spreadsheet URL:', ssUrl);
    }
    
    // Step 4: Test core functions
    console.log('Step 3: Testing core functions...');
    
    // Test BigQuery connection
    try {
      const testQuery = BigQuery.Jobs.query({
        query: 'SELECT 1 as test',
        useLegacySql: false
      }, CONFIG.PROJECT_ID);
      console.log('✅ BigQuery connection verified');
    } catch (e) {
      console.error('❌ BigQuery connection failed:', e.message);
    }
    
    // Step 5: Initialize triggers
    console.log('Step 4: Installing triggers...');
    installTriggers();
    console.log('✅ Triggers installed');
    
    // Step 6: Initialize caption banks
    console.log('Step 5: Initializing caption banks...');
    try {
      initializeCaptionBanks();
      console.log('✅ Caption banks initialized');
    } catch (e) {
      console.log('⚠️ Caption banks initialization failed:', e.message);
    }
    
    // Step 7: Create initial data
    console.log('Step 6: Loading initial data...');
    try {
      refreshWeekPlan();
      console.log('✅ Week plan loaded');
    } catch (e) {
      console.log('⚠️ Week plan load failed (may need manual run):', e.message);
    }
    
    console.log('');
    console.log('========================================');
    console.log('✨ EROS Scheduler Hub Setup Complete!');
    console.log('========================================');
    console.log('');
    console.log('Next steps:');
    console.log('1. Open the spreadsheet using the URL above');
    console.log('2. Share with schedulers (Editor or Viewer access)');
    console.log('3. Test the menu functions');
    console.log('');
    
    return {
      success: true,
      spreadsheetUrl: SpreadsheetApp.getActiveSpreadsheet().getUrl(),
      spreadsheetId: SpreadsheetApp.getActiveSpreadsheet().getId()
    };
    
  } catch (error) {
    console.error('Setup failed:', error);
    return {
      success: false,
      error: error.message
    };
  }
}

// Helper function to ensure proper authorization
function ensureAuthorization() {
  try {
    // Test all required scopes
    SpreadsheetApp.getActiveSpreadsheet();
    BigQuery.Jobs.query({query: 'SELECT 1', useLegacySql: false}, 'of-scheduler-proj');
    ScriptApp.getProjectTriggers();
    Session.getActiveUser().getEmail();
    
    console.log('✅ All permissions authorized');
    return true;
  } catch (e) {
    console.log('❌ Authorization needed:', e.message);
    return false;
  }
}

// Run the complete setup
function runCompleteSetup() {
  console.log('🎯 Running Complete EROS Scheduler Hub Setup');
  console.log('============================================');
  
  // First ensure authorization
  if (!ensureAuthorization()) {
    console.log('⚠️ Please authorize the script and run again');
    return;
  }
  
  // Run the setup
  const result = setupSchedulerHub();
  
  if (result.success) {
    console.log('');
    console.log('🎉 SUCCESS! Your scheduler hub is ready.');
    console.log('');
    console.log('📊 SHARE THIS WITH YOUR TEAM:');
    console.log('Spreadsheet URL:', result.spreadsheetUrl);
  } else {
    console.log('');
    console.log('❌ Setup encountered issues. Please check the logs.');
  }
}