/**
 * ML-POWERED CAPTION SELECTOR FOR GOOGLE SHEETS
 * ==============================================
 * Updated to use new BigQuery ML architecture
 * 
 * USAGE:
 * =getCaptions(username, page_type, num_captions, strategy)
 * 
 * PARAMETERS:
 * - username: Creator's username
 * - page_type: "free" or "paid"
 * - num_captions: Number of captions to return (default: 5)
 * - strategy: "balanced", "explore", or "exploit" (default: "balanced")
 * 
 * EXAMPLES:
 * =getCaptions("jane_doe", "free", 10, "balanced")
 * =getCaptions("john_smith", "paid", 5, "explore")
 */

// Configuration
const PROJECT_ID = 'of-scheduler-proj';
const CACHE_DURATION = 60; // Cache results for 60 seconds

/**
 * Main function to get ML-ranked captions
 */
function getCaptions(username, page_type = "free", num_captions = 5, strategy = "balanced") {
  // Validate inputs
  if (!username) {
    throw new Error("Username is required");
  }
  
  // Build cache key
  const cacheKey = `captions_${username}_${page_type}_${num_captions}_${strategy}`;
  
  // Check cache
  const cache = CacheService.getScriptCache();
  const cached = cache.get(cacheKey);
  if (cached) {
    return JSON.parse(cached);
  }
  
  try {
    // Query the new ML export layer
    const query = buildQuery(username, page_type, num_captions, strategy);
    const results = runBigQuery(query);
    
    // Format results for display
    const formatted = formatResults(results);
    
    // Cache results
    cache.put(cacheKey, JSON.stringify(formatted), CACHE_DURATION);
    
    return formatted;
    
  } catch (error) {
    console.error('Error fetching captions:', error);
    throw new Error(`Failed to fetch captions: ${error.message}`);
  }
}

/**
 * Build BigQuery SQL based on strategy
 */
function buildQuery(username, page_type, num_captions, strategy) {
  const username_page = `${username}_${page_type}`;
  
  // Different epsilon values for different strategies
  const epsilonMap = {
    'explore': 0.25,
    'balanced': 0.10,
    'exploit': 0.05
  };
  const epsilon = epsilonMap[strategy] || 0.10;
  
  return `
    WITH recommendations AS (
      SELECT
        caption_id,
        caption_text,
        final_score,
        expected_rps,
        confidence_score,
        exploration_bonus,
        is_eligible,
        hours_since_use,
        total_sends,
        performance_tier,
        
        -- Apply epsilon-greedy selection
        CASE
          WHEN RAND() < ${epsilon} THEN RAND()  -- Exploration
          ELSE final_score                       -- Exploitation
        END AS selection_score
        
      FROM \`${PROJECT_ID}.layer_07_export.schedule_recommendations\`
      WHERE username_page = '${username_page}'
        AND schedule_date = CURRENT_DATE()
        AND is_eligible = TRUE
    )
    SELECT
      caption_text,
      ROUND(expected_rps, 2) AS expected_rps,
      ROUND(confidence_score * 100, 1) AS confidence_pct,
      performance_tier,
      CASE 
        WHEN total_sends < 10 THEN 'New'
        WHEN total_sends < 50 THEN 'Testing'
        ELSE 'Proven'
      END AS status,
      CAST(hours_since_use AS INT64) AS hours_since_use
    FROM recommendations
    ORDER BY selection_score DESC
    LIMIT ${num_captions}
  `;
}

/**
 * Execute BigQuery query
 */
function runBigQuery(query) {
  const request = {
    query: query,
    useLegacySql: false,
    location: 'US'
  };
  
  const queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
  
  if (!queryResults.rows || queryResults.rows.length === 0) {
    return [];
  }
  
  // Convert rows to objects
  return queryResults.rows.map(row => {
    const obj = {};
    queryResults.schema.fields.forEach((field, index) => {
      obj[field.name] = row.f[index].v;
    });
    return obj;
  });
}

/**
 * Format results for spreadsheet display
 */
function formatResults(results) {
  if (results.length === 0) {
    return [['No eligible captions found']];
  }
  
  // Create header row
  const headers = [
    'Caption Text',
    'Expected RPS',
    'Confidence %',
    'Tier',
    'Status',
    'Hours Since Use'
  ];
  
  // Create data rows
  const rows = results.map(r => [
    r.caption_text,
    parseFloat(r.expected_rps),
    parseFloat(r.confidence_pct),
    r.performance_tier,
    r.status,
    parseInt(r.hours_since_use)
  ]);
  
  return [headers, ...rows];
}

/**
 * Get system health status
 */
function getSystemHealth() {
  const query = `
    SELECT
      health_score,
      alert_status,
      feature_store_age_hours,
      eligible_rate,
      daily_sends
    FROM \`${PROJECT_ID}.ops_monitor.dashboard_system_health\`
  `;
  
  const results = runBigQuery(query);
  
  if (results.length === 0) {
    return [['System health data unavailable']];
  }
  
  const health = results[0];
  return [
    ['Metric', 'Value'],
    ['Health Score', parseInt(health.health_score)],
    ['Status', health.alert_status],
    ['Data Age (hours)', parseInt(health.feature_store_age_hours)],
    ['Eligibility Rate', parseFloat(health.eligible_rate).toFixed(2)],
    ['Daily Sends', parseInt(health.daily_sends)]
  ];
}

/**
 * Get top performing captions across system
 */
function getTopCaptions(limit = 10) {
  const query = `
    SELECT
      caption_id,
      username_page,
      rps,
      confidence,
      recent_sends,
      overall_rank,
      status
    FROM \`${PROJECT_ID}.ops_monitor.dashboard_top_captions\`
    LIMIT ${limit}
  `;
  
  const results = runBigQuery(query);
  
  if (results.length === 0) {
    return [['No caption data available']];
  }
  
  const headers = [
    'Caption ID',
    'Page',
    'RPS',
    'Confidence',
    'Recent Sends',
    'Rank',
    'Status'
  ];
  
  const rows = results.map(r => [
    r.caption_id,
    r.username_page,
    parseFloat(r.rps),
    parseFloat(r.confidence),
    parseInt(r.recent_sends),
    parseInt(r.overall_rank),
    r.status
  ]);
  
  return [headers, ...rows];
}

/**
 * Quick caption lookup by ID
 */
function lookupCaption(caption_id) {
  const query = `
    SELECT
      caption_text,
      username_page,
      performance_tier,
      is_available,
      expected_rps,
      confidence_score,
      days_since_use
    FROM \`${PROJECT_ID}.layer_07_export.api_caption_lookup\`
    WHERE caption_id = '${caption_id}'
  `;
  
  const results = runBigQuery(query);
  
  if (results.length === 0) {
    return [['Caption not found']];
  }
  
  const caption = results[0];
  return [
    ['Property', 'Value'],
    ['Text', caption.caption_text],
    ['Page', caption.username_page],
    ['Tier', caption.performance_tier],
    ['Available', caption.is_available === 'true' ? 'Yes' : 'No'],
    ['Expected RPS', parseFloat(caption.expected_rps)],
    ['Confidence', parseFloat(caption.confidence_score)],
    ['Days Since Use', parseInt(caption.days_since_use)]
  ];
}

/**
 * Custom menu for easy access
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('ML Caption System')
    .addItem('Get System Health', 'showSystemHealth')
    .addItem('View Top Captions', 'showTopCaptions')
    .addSeparator()
    .addItem('Help', 'showHelp')
    .addToUi();
}

/**
 * Show system health in new sheet
 */
function showSystemHealth() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName('System Health');
  
  if (!sheet) {
    sheet = ss.insertSheet('System Health');
  }
  
  sheet.clear();
  const data = getSystemHealth();
  sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
  
  // Format header
  sheet.getRange(1, 1, 1, data[0].length)
    .setBackground('#4285f4')
    .setFontColor('#ffffff')
    .setFontWeight('bold');
  
  sheet.autoResizeColumns(1, data[0].length);
}

/**
 * Show top captions in new sheet
 */
function showTopCaptions() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName('Top Captions');
  
  if (!sheet) {
    sheet = ss.insertSheet('Top Captions');
  }
  
  sheet.clear();
  const data = getTopCaptions(25);
  sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
  
  // Format header
  sheet.getRange(1, 1, 1, data[0].length)
    .setBackground('#4285f4')
    .setFontColor('#ffffff')
    .setFontWeight('bold');
  
  sheet.autoResizeColumns(1, data[0].length);
}

/**
 * Show help information
 */
function showHelp() {
  const html = HtmlService.createHtmlOutput(`
    <div style="font-family: Arial, sans-serif; padding: 20px;">
      <h2>ML Caption Selector Help</h2>
      
      <h3>Functions:</h3>
      <ul>
        <li><strong>=getCaptions(username, page_type, num, strategy)</strong><br>
            Get ML-ranked captions for a creator</li>
        <li><strong>=getSystemHealth()</strong><br>
            Check ML system health status</li>
        <li><strong>=getTopCaptions(limit)</strong><br>
            View top performing captions</li>
        <li><strong>=lookupCaption(caption_id)</strong><br>
            Look up details for a specific caption</li>
      </ul>
      
      <h3>Strategies:</h3>
      <ul>
        <li><strong>explore</strong> - 25% random selection for testing new captions</li>
        <li><strong>balanced</strong> - 10% random selection (default)</li>
        <li><strong>exploit</strong> - 5% random selection for maximum performance</li>
      </ul>
      
      <h3>Examples:</h3>
      <code>=getCaptions("jane_doe", "free", 10, "balanced")</code><br>
      <code>=getTopCaptions(20)</code><br>
      <code>=lookupCaption("CAP_12345")</code>
      
      <h3>Menu Options:</h3>
      <p>Use the "ML Caption System" menu for quick access to system health and top captions.</p>
    </div>
  `)
  .setWidth(500)
  .setHeight(600);
  
  SpreadsheetApp.getUi().showModalDialog(html, 'ML Caption Selector Help');
}

/**
 * Test function for development
 */
function testGetCaptions() {
  const result = getCaptions("sample_user", "free", 5, "balanced");
  console.log(result);
}