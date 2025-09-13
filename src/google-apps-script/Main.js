/*
 * EROS SCHEDULER HUB — PAGE-AWARE v7.1 (Top-N Picker, BQ precompute)
 * Project: of-scheduler-proj
 *
 * Design:
 *  - Thin Sheet, Thick BigQuery (materialized Top-N per slot → fast picker)
 *  - Uses canonical username/page model (username_page = username__page_type)
 *  - Adds "Load My Day (All)" = PPV + Follow-ups + DripSet (MM+Wall) + Renewals + Link drops
 *  - Installable onEdit for idempotent logging to ops.send_log
 *
 * Prereqs:
 *  - Enable Advanced Service “BigQuery API” in Apps Script project
 */

// ==================== CONFIG ====================
const CONFIG = {
  PROJECT_ID: 'of-scheduler-proj',
  DATASET_MART: 'mart',
  DATASET_CORE: 'core',
  DATASET_OPS:  'ops',
  DATASET_SHEETS: 'sheets',

  // Materialized next-24h caption ranks (partitioned by DATE(slot_dt_local))
  RANK_TABLE: 'mart.caption_rank_next24_v3_tbl',

  // Union view with PPV + follow-ups + non-DM (today) for My Day (All)
  MY_DAY_ALL_VIEW: 'sheets.v_my_day_slots_all_v1',

  // Paid-gated next-24 (wrapper that drops PPV on free pages)
  NEXT24_VIEW_GATED: 'mart.v_slot_recommendations_next24_gated_v1',

  // Feature switch: enforce paid-gated next-24 in “✅ My Day”
  ENFORCE_GATED_IN_MY_DAY: true,

  RANK_TOPN: 10, // how many to show in picker

  SHEETS: {
    WEEK_PLAN: '📅 Week Plan',
    MY_DAY:    '✅ My Day',
    BRIEF:     '📋 Brief',
    ALERTS:    '⚠️ Alerts',
    SETTINGS:  '⚙️ Settings',
    LOG:       '📝 Activity Log'
  },
  CACHE_TTL: { RECO: 3600, BRIEF: 7200, USER: 3600 },
  COOLDOWN_DAYS: 28,
  COLORS: {
    HEADER: '#1e40af',
    RECOMMENDED: '#dcfce7',
    MODIFIED: '#fef3c7',
    SENT: '#dbeafe',
    ALERT: '#fee2e2',
    LOCKED: '#f3f4f6',
    CAPTION_SELECTED: '#e0e7ff'
  }
};

// Centralized column map (prevents off-by-one bugs)
const IDX = {
  WEEK: { DATE:1, DAY:2, MODEL:3, PAGE:4, SLOT:5, REC_TIME:6, REC_PRICE:7, CAPTION_ID:8, CAPTION_PREV:9, ACT_TIME:10, ACT_PRICE:11, STATUS:12, REASON:13, SCORE:14, LOCK:15, HASH:16, UPAGE:17 },
  DAY:  { TIME:1, MODEL:2, PAGE:3, PRICE:4, CAPTION_ID:5, CAPTION_PREV:6, PICK:7, STATUS:8, NOTES:9, HASH:10, UPAGE:11, HOD:12, SLOT_RANK:13 }
};

/** Safe user email helper */
function getCurrentUserEmail_(){
  try{ var e = Session.getActiveUser().getEmail(); if (e) return e.toLowerCase(); }catch(_){}
  try{ var e2 = Session.getEffectiveUser().getEmail(); if (e2) return e2.toLowerCase(); }catch(_){}
  return '';
}

// ==================== MENU & INIT ====================
function onOpen(){
  try{
    const ui = SpreadsheetApp.getUi();
    ui.createMenu('🚀 Scheduler Hub')
      .addItem('📅 Refresh Week Plan','refreshWeekPlan')
      .addItem('✅ Load My Day','loadMyDay')
      .addItem('✅ Load My Day (All)','loadMyDayAll')
      .addSeparator()
      .addItem('🧠 Pick caption for row','showCaptionPicker')
      .addItem('🏦 Override with Caption Bank','showCaptionBankPicker')
      .addItem('📈 My Override Performance','showOverrideFeedback')
      .addItem('🔄 Sync with BigQuery','syncWithBigQuery')
      .addSeparator()
      .addSubMenu(ui.createMenu('📚 Caption Banks')
        .addItem('Initialize All Banks','initializeCaptionBanks')
        .addItem('Refresh All Banks','refreshAllCaptionBanks')
        .addItem('Search Captions','searchCaptionBanks'))
      .addSeparator()
      .addItem('📊 Generate Brief','generateBrief')
      .addItem('⚠️ Check Alerts','checkAlerts')
      .addSeparator()
      .addItem('📤 Submit Daily Plan','submitDailyPlan')
      .addItem('📝 View Activity Log','viewActivityLog')
      .addSeparator()
      .addItem('⚙️ Settings','openSettings')
      .addItem('❓ Help','showHelp')
      .addToUi();
    initializeSheets();
    installTriggers();
  }catch(err){ handleError(err,'onOpen'); }
}

function initializeSheets(){
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const names = Object.keys(CONFIG.SHEETS).map(function(k){ return CONFIG.SHEETS[k]; });
  names.forEach(function(name){
    var sh = ss.getSheetByName(name);
    if (!sh){ sh = ss.insertSheet(name); setupSheetHeaders(sh, name); }
  });
  applyProtections();
}

function setupSheetHeaders(sheet, name){
  const H = {};
  H[CONFIG.SHEETS.WEEK_PLAN] = ['Date','Day','Model','Page','Slot','Rec Time','Rec Price','Caption ID','Caption Preview','Actual Time','Actual Price','Status','Reason','Score','Lock','Tracking Hash','Username Page'];
  H[CONFIG.SHEETS.MY_DAY]    = ['Time','Model','Page','Price','Caption ID','Caption Preview','Pick','Status','Notes','Tracking Hash','Username Page','Hour','Slot Rank'];
  H[CONFIG.SHEETS.BRIEF]     = ['Model','Page','State','Fans','Revenue 7D','Best Hours','Price Band','Top Captions','Avoid Count'];
  H[CONFIG.SHEETS.ALERTS]    = ['Priority','Type','Model','Page','Message','Action Required'];
  H[CONFIG.SHEETS.LOG]       = ['Timestamp','User','Action','Model','Page','Details','Result'];

  if (H[name]){
    const row = H[name];
    sheet.getRange(1,1,1,row.length).setValues([row]).setBackground(CONFIG.COLORS.HEADER).setFontColor('#fff').setFontWeight('bold');
    sheet.setFrozenRows(1);
  }
  if (name===CONFIG.SHEETS.WEEK_PLAN){ try{ sheet.hideColumns(IDX.WEEK.HASH, 2); }catch(_){ } }
  if (name===CONFIG.SHEETS.MY_DAY){   try{ sheet.hideColumns(IDX.DAY.HASH, 4);  }catch(_){ } }
}

function applyProtections(){
  // (optional) add range protections here if needed
}

// ==================== BigQuery Service ====================
class BigQueryService{
  constructor(){ this.projectId = CONFIG.PROJECT_ID; this.cache = CacheService.getUserCache(); }
  query(sql, params, useCache, ttl){
    params = params||[]; useCache = (useCache===undefined)?true:useCache; ttl = ttl||CONFIG.CACHE_TTL.RECO;
    const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, sql + JSON.stringify(params));
    const key = digest.map(function(b){ b=(b+256)%256; var s=b.toString(16); return s.length===1?'0'+s:s; }).join('');
    if (useCache){ const cached = this.cache.get(key); if (cached) return JSON.parse(cached); }
    const req = { query: sql, useLegacySql:false };
    if (params.length){ req.parameterMode='NAMED'; req.queryParameters=params; }
    const res = BigQuery.Jobs.query(req, this.projectId);
    const fields = (res.schema && res.schema.fields)? res.schema.fields.map(function(f){return f.name;}):[];
    const rows = (res.rows||[]).map(function(r){ const o={}; r.f.forEach(function(c,i){ o[fields[i]]=c.v; }); return o; });
    if (useCache){ this.cache.put(key, JSON.stringify(rows), ttl); }
    return rows;
  }
  exec(sql, params){ return this.query(sql, params||[], false); }
}

// ==================== Engine ====================
class RecommendationEngine{
  constructor(){ this.bq = new BigQueryService(); this.userEmail = getCurrentUserEmail_(); }

  getSchedulerInfo(){
    const sql = [
      'SELECT scheduler_name, display_name, IFNULL(can_view_all,FALSE) AS can_view_all',
      'FROM `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_CORE+'.scheduler_user_map`',
      'WHERE LOWER(email)=@e',
      'LIMIT 1'
    ].join('\n');
    const r = this.bq.query(sql,[{name:'e', parameterType:{type:'STRING'}, parameterValue:{value:this.userEmail}}], false);
    return r[0]||null;
  }

  getWeeklyRecommendations(schedulerName, startDate, endDate, canViewAll){
    var where = canViewAll
      ? 'WHERE t.date_local BETWEEN @start_date AND @end_date'
      : 'WHERE LOWER(t.scheduler_name)=LOWER(@scheduler_name) AND t.date_local BETWEEN @start_date AND @end_date';

    // Use CORE final pages view
    const sql = [
      'WITH base AS (',
      '  SELECT t.username_std, t.page_type, t.username_page, t.scheduler_name, t.date_local, t.slot_rank,',
      '         t.hod_local AS recommended_hour, t.price_usd AS recommended_price, t.tracking_hash,',
      "         CASE WHEN t.slot_rank=0 THEN 'Prime slot - highest performance'",
      "              WHEN t.slot_rank=1 THEN 'Secondary peak hour'",
      "              ELSE 'Coverage slot' END AS recommendation_reason",
      '  FROM `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_CORE+'.v_weekly_template_7d_pages_final` t',
      '  '+where,
      ')',
      'SELECT b.*, COALESCE(h.score,0) AS slot_score',
      'FROM base b',
      'LEFT JOIN `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_MART+'.v_mm_dow_hod_180d_local_v2` h',
      '  ON h.username_std=b.username_std AND h.hod_local=b.recommended_hour AND h.dow_local=MOD(EXTRACT(DAYOFWEEK FROM b.date_local)+5,7)',
      'ORDER BY b.date_local, b.username_std, b.page_type, b.slot_rank'
    ].join('\n');

    const params = [
      {name:'start_date', parameterType:{type:'DATE'}, parameterValue:{value:startDate}},
      {name:'end_date',   parameterType:{type:'DATE'}, parameterValue:{value:endDate}}
    ];
    if (!canViewAll){ params.unshift({name:'scheduler_name', parameterType:{type:'STRING'}, parameterValue:{value:schedulerName}}); }
    return this.bq.query(sql, params, false);
  }
}

// ==================== Week Plan & My Day (PPV-only) ====================
function refreshWeekPlan(){
  try{
    const ss = SpreadsheetApp.getActiveSpreadsheet(); const sheet = ss.getSheetByName(CONFIG.SHEETS.WEEK_PLAN);
    if (!sheet) throw new Error('Week Plan sheet not found');
    const sched = new RecommendationEngine().getSchedulerInfo();
    if (!sched){ SpreadsheetApp.getUi().alert('Your email is not registered as a scheduler.'); return; }

    const lastRow = sheet.getLastRow(); if (lastRow>1){ sheet.getRange(2,1,lastRow-1, Math.max(sheet.getLastColumn(),IDX.WEEK.UPAGE)).clear({contentsOnly:true}); }

    const today = new Date();
    const startDate = Utilities.formatDate(today, Session.getScriptTimeZone(), 'yyyy-MM-dd');
    const endDate   = Utilities.formatDate(new Date(today.getTime()+6*24*60*60*1000), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    const canAll = (sched.can_view_all===true || sched.can_view_all==='true');

    const recs = new RecommendationEngine().getWeeklyRecommendations(sched.scheduler_name, startDate, endDate, canAll);
    if (!recs.length){ SpreadsheetApp.getUi().alert('No recommendations found for your assigned pages.'); return; }

    const rows = recs.map(function(r){
      const dayName = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][new Date(r.date_local+'T00:00:00').getDay()];
      return [ r.date_local, dayName, r.username_std, (r.page_type||'main'), 'Slot '+(Number(r.slot_rank)+1),
               String(r.recommended_hour)+':00', Number(r.recommended_price)||0, '', '', '', '',
               'Planned', r.recommendation_reason, Number(r.slot_score)||0, false, r.tracking_hash, r.username_page ];
    });

    sheet.getRange(2,1,rows.length, rows[0].length).setValues(rows);
    applyWeekPlanFormatting(sheet, rows.length);
    try{ sheet.hideColumns(IDX.WEEK.HASH,2); }catch(_){ }
    SpreadsheetApp.getUi().alert('Week plan loaded: '+rows.length+' slots. Use "Pick caption for row" to choose Top-N.');
  }catch(e){ handleError(e,'refreshWeekPlan'); }
}

// ==================== My Day (PPV-only, with paid gating enforcement) ====================
function loadMyDay(){
  try{
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const week = ss.getSheetByName(CONFIG.SHEETS.WEEK_PLAN);
    const day  = ss.getSheetByName(CONFIG.SHEETS.MY_DAY);
    if (!week||!day) throw new Error('Required sheets not found');

    // clear existing
    const lr = day.getLastRow();
    if (lr>1){ day.getRange(2,1, lr-1, Math.max(day.getLastColumn(),IDX.DAY.SLOT_RANK)).clear({contentsOnly:true}); }

    const tz = Session.getScriptTimeZone();
    const todayISO = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');
    const usedLastRow = week.getLastRow();
    if (usedLastRow<=1){ SpreadsheetApp.getUi().alert("Today's schedule loaded: 0 sends"); return; }
    const W = week.getRange(2,1, usedLastRow-1, Math.max(week.getLastColumn(),IDX.WEEK.UPAGE)).getValues();

    // Build allowed PPV pairs (username_page, hod) for today from the gated next-24 view
    const allowedPPV = {};
    if (CONFIG.ENFORCE_GATED_IN_MY_DAY){
      const bq = new BigQueryService();
      const sqlAllowed = [
        'SELECT username_page, EXTRACT(HOUR FROM slot_dt_local) AS hod',
        'FROM `'+CONFIG.PROJECT_ID+'.'+CONFIG.NEXT24_VIEW_GATED+'`',
        'WHERE DATE(slot_dt_local) = CURRENT_DATE()'
      ].join('\n');
      try{
        const rowsAllowed = bq.query(sqlAllowed, [], false, 300);
        rowsAllowed.forEach(function(r){
          const up = String(r.username_page||''); const h = Number(r.hod||0);
          if (!allowedPPV[up]) allowedPPV[up] = {};
          allowedPPV[up][h] = true;
        });
      }catch(e){ console.warn('Could not fetch gated pairs; proceeding without enforcement', e); }
    }

    // group week rows for today
    const buckets = {}; const pagesSet = {};
    for (var i=0;i<W.length;i++){
      var r=W[i]; if (!r[IDX.WEEK.DATE-1]) continue;
      var rowDateISO = Utilities.formatDate(new Date(r[IDX.WEEK.DATE-1]), tz, 'yyyy-MM-dd');
      if (rowDateISO!==todayISO) continue;
      var model=r[IDX.WEEK.MODEL-1], page=r[IDX.WEEK.PAGE-1], up=r[IDX.WEEK.UPAGE-1]||(model+'__'+page);
      if (!buckets[up]) buckets[up]={model:model, page:page, rows:[]};
      buckets[up].rows.push(r); pagesSet[up]=true;
    }

    var pages = Object.keys(pagesSet).sort(function(a,b){
      var A=buckets[a], B=buckets[b];
      var m=String(A.model).localeCompare(String(B.model));
      return m!==0?m:String(A.page).localeCompare(String(B.page));
    });

    const out=[]; const headerRows=[];
    pages.forEach(function(up){
      var b=buckets[up]; if (!b || !b.rows.length) return;
      var label=b.model+' — '+b.page+' ('+b.rows.length+' sends)';
      out.push([label,'','','','','','','','','','','','']); headerRows.push(2+out.length-1);

      b.rows.sort(function(r1,r2){
        var h1=parseInt(String(r1[IDX.WEEK.REC_TIME-1]).split(':')[0],10)||0;
        var h2=parseInt(String(r2[IDX.WEEK.REC_TIME-1]).split(':')[0],10)||0;
        return h1-h2;
      });

      b.rows.forEach(function(r){
        var recTime = String(r[IDX.WEEK.REC_TIME-1]||'');
        var hod     = parseInt(recTime.split(':')[0],10)||0;
        var price   = Number(r[IDX.WEEK.REC_PRICE-1]||0);
        var capId   = r[IDX.WEEK.CAPTION_ID-1]||'';
        var capPrev = r[IDX.WEEK.CAPTION_PREV-1]||'';
        var pick    = '🧠 Pick';    // default picker (DM)
        var status  = r[IDX.WEEK.STATUS-1] || 'Planned';
        var notes   = r[IDX.WEEK.REASON-1] || '';

        // Enforce paid gating at display time
        if (CONFIG.ENFORCE_GATED_IN_MY_DAY && price>0){
          var allowed = !!(allowedPPV[up] && allowedPPV[up][hod]);
          if (!allowed){
            price = 0;
            pick  = '';                 // no picker for FREE
            notes = (notes? (notes+'; ') : '') + 'auto-gated';
          }
        }

        var slotRank = parseInt(String(r[IDX.WEEK.SLOT-1]).replace(/[^0-9]/g,''),10)-1||0;
        var hash     = r[IDX.WEEK.HASH-1];
        var model    = r[IDX.WEEK.MODEL-1];
        var page     = r[IDX.WEEK.PAGE-1];

        out.push([ recTime, model, page, price, capId, capPrev, pick, status, notes, hash, up, hod, slotRank ]);
      });
    });

    if (out.length){
      day.getRange(2,1,out.length,out[0].length).setValues(out);
      applyMyDayFormatting(day, out.length, { grouped:true });
      headerRows.forEach(function(r){ try{
        day.getRange(r,IDX.DAY.PICK,1,1).clearDataValidations().clearContent();
        day.getRange(r,IDX.DAY.STATUS,1,1).clearDataValidations().setValue('');
        day.getRange(r,1,1,6).merge();
        day.getRange(r,1,1,9).setBackground(CONFIG.COLORS.LOCKED).setFontWeight('bold').setHorizontalAlignment('left');
      }catch(_){ }});
      try{ day.hideColumns(IDX.DAY.HASH,4); }catch(_){ }
    }
    var count = out.length - headerRows.length;
    SpreadsheetApp.getUi().alert("Today's schedule loaded: "+count+" sends.\n(🧠 appears only on DM rows allowed by gating.)");
  }catch(e){ handleError(e,'loadMyDay'); }
}

// ==================== My Day (ALL) — PPV + followups + DripSet + Renewals + Link drops ====================
function loadMyDayAll(){
  try{
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const day = ss.getSheetByName(CONFIG.SHEETS.MY_DAY);
    if (!day) throw new Error('My Day sheet not found');

    // clear existing
    const lr = day.getLastRow();
    if (lr>1){ day.getRange(2,1, lr-1, Math.max(day.getLastColumn(),IDX.DAY.SLOT_RANK)).clear({contentsOnly:true}); }

    const sql = [
      'SELECT username_std, username_page, slot_kind, channel,',
      '       slot_local_human, hod, IFNULL(price_usd,0) AS price_usd, slot_rank',
      'FROM `'+CONFIG.PROJECT_ID+'.'+CONFIG.MY_DAY_ALL_VIEW+'`',
      'ORDER BY username_std, slot_local_human, slot_kind'
    ].join('\n');

    const rows = new BigQueryService().query(sql, [], false, 600);
    if (!rows.length){ SpreadsheetApp.getUi().alert('No items for today.'); return; }

    // Map into MY_DAY columns:
    // ['Time','Model','Page','Price','Caption ID','Caption Preview','Pick','Status','Notes','Tracking Hash','Username Page','Hour','Slot Rank']
    const out = rows.map(function(r){
      const timeVal  = r.slot_local_human;
      const model    = r.username_std;
      const page     = String(r.username_page||'').split('__')[1] || 'main';
      const price    = Number(r.price_usd||0);
      const capId    = '';
      const capPrev  = '';
      const pick     = (String(r.channel).toUpperCase()==='MM') ? '🧠 Pick' : '';  // picker only on DM
      const status   = 'Planned';
      const notes    = r.slot_kind; // 'ppv','ppv_followup','drip_mm','drip_wall','renewal','link_drop'
      const upage    = r.username_page || (r.username_std+'__'+page);
      const hod      = Number(r.hod||0);
      const slotRank = (r.slot_rank!=null) ? Number(r.slot_rank) : '';
      const hash     = makeTrackingHash_([upage, timeVal, notes].join('|')); // deterministic row key

      return [ timeVal, model, page, price, capId, capPrev, pick, status, notes, hash, upage, hod, slotRank ];
    });

    day.getRange(2,1,out.length,out[0].length).setValues(out);
    applyMyDayFormatting(day, out.length, { grouped:false });

    SpreadsheetApp.getUi().alert("Today's full schedule loaded: "+out.length+" entries.\n(Use 🧠 Pick on DM rows only; Wall/Link rows have no picker.)");
  }catch(e){ handleError(e,'loadMyDayAll'); }
}

// Deterministic hex hash per row (for dedupe in ops.send_log)
function makeTrackingHash_(s){
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, s);
  return bytes.map(function(b){ b=(b+256)%256; var h=b.toString(16); return h.length===1?'0'+h:h; }).join('');
}

function applyWeekPlanFormatting(sheet, n){
  sheet.getRange(2,IDX.WEEK.REC_PRICE,n,1).setNumberFormat('$0.00');
  sheet.getRange(2,IDX.WEEK.ACT_PRICE,n,1).setNumberFormat('$0.00');
  sheet.getRange(2,IDX.WEEK.SCORE,n,1).setNumberFormat('0');
  const status = sheet.getRange(2,IDX.WEEK.STATUS,n,1);
  const rules=[];
  rules.push(SpreadsheetApp.newConditionalFormatRule().whenTextEqualTo('Planned').setBackground(CONFIG.COLORS.RECOMMENDED).setRanges([status]).build());
  rules.push(SpreadsheetApp.newConditionalFormatRule().whenTextEqualTo('Sent').setBackground(CONFIG.COLORS.SENT).setRanges([status]).build());
  rules.push(SpreadsheetApp.newConditionalFormatRule().whenTextEqualTo('Modified').setBackground(CONFIG.COLORS.MODIFIED).setRanges([status]).build());
  sheet.setConditionalFormatRules(rules);
  status.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(['Planned','Modified','Sent','Skipped'], true).build());
  sheet.getRange(2,IDX.WEEK.LOCK,n,1).insertCheckboxes();
}

function applyMyDayFormatting(sheet, n, opts){
  opts=opts||{};
  sheet.getRange(2,IDX.DAY.TIME,n,1).setNumberFormat('h:mm am/pm');
  sheet.getRange(2,IDX.DAY.PRICE,n,1).setNumberFormat('$0.00');
  sheet.getRange(2,IDX.DAY.TIME,n,1).setFontWeight('bold').setFontSize(12);
  const pick = sheet.getRange(2,IDX.DAY.PICK,n,1);
  pick.setHorizontalAlignment('center').setFontWeight('bold').setBackground('#e0e7ff').setFontColor('#667eea').setBorder(true,true,true,true,false,false);
  const status = sheet.getRange(2,IDX.DAY.STATUS,n,1);
  status.setDataValidation(SpreadsheetApp.newDataValidation().requireValueInList(['Planned','Ready','Sent','Skipped'], true).build());
  if (!opts.grouped){ sheet.getRange(2,1,n, sheet.getLastColumn()).sort({column:IDX.DAY.TIME, ascending:true}); }
}

// ==================== Caption Picker (materialized Top-N) ====================
function showCaptionPicker(){
  try{
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sh = ss.getActiveSheet();
    const name = sh.getName();
    if (name!==CONFIG.SHEETS.MY_DAY && name!==CONFIG.SHEETS.WEEK_PLAN){ SpreadsheetApp.getUi().alert('Select a row in My Day or Week Plan'); return; }
    const row = sh.getActiveRange().getRow(); if (row<=1){ SpreadsheetApp.getUi().alert('Select a data row'); return; }

    const tz = Session.getScriptTimeZone();
    let model, page, up, hod, price;

    if (name===CONFIG.SHEETS.MY_DAY){
      model = String(sh.getRange(row,IDX.DAY.MODEL).getValue());
      page  = String(sh.getRange(row,IDX.DAY.PAGE ).getValue());
      price = Number(sh.getRange(row,IDX.DAY.PRICE).getValue()||0);
      up    = String(sh.getRange(row,IDX.DAY.UPAGE).getValue() || (model+'__'+page));
      hod   = Number(sh.getRange(row,IDX.DAY.HOD  ).getValue())
              || parseInt(String(sh.getRange(row,IDX.DAY.TIME).getValue()).split(':')[0],10) || 0;
    }else{
      model = String(sh.getRange(row,IDX.WEEK.MODEL).getValue());
      page  = String(sh.getRange(row,IDX.WEEK.PAGE ).getValue());
      price = Number(sh.getRange(row,IDX.WEEK.REC_PRICE).getValue()||0);
      up    = String(sh.getRange(row,IDX.WEEK.UPAGE).getValue() || (model+'__'+page));
      hod   = parseInt(String(sh.getRange(row,IDX.WEEK.REC_TIME).getValue()).split(':')[0],10) || 0;
    }

    const bq = new BigQueryService();
    const baseTbl = '`'+CONFIG.PROJECT_ID+'.'+CONFIG.RANK_TABLE+'`';

    // Nearest slot for same HOD; fallback to earliest upcoming slot for the page (any HOD)
    const sqlNearest = [
      'WITH nearest AS (',
      '  SELECT slot_dt_local',
      '  FROM '+baseTbl,
      '  WHERE username_page=@up AND hod=@hod',
      '  ORDER BY slot_dt_local ASC',
      '  LIMIT 1',
      ')',
      'SELECT caption_id, SUBSTR(COALESCE(caption_text, \'\'), 1, 500) AS caption_text,',
      '       p_buy_eb, rps_eb_price, se_bonus, style_score,',
      '       (rps_eb_price + se_bonus + COALESCE(style_score,0)) AS score_final,',
      '       rn',
      'FROM '+baseTbl,
      'WHERE username_page=@up AND hod=@hod',
      '  AND slot_dt_local = (SELECT slot_dt_local FROM nearest)',
      'ORDER BY rn',
      'LIMIT @topn'
    ].join('\n');
    const paramsNearest = [
      {name:'up',   parameterType:{type:'STRING'}, parameterValue:{value:up}},
      {name:'hod',  parameterType:{type:'INT64'},  parameterValue:{value:String(hod)}},
      {name:'topn', parameterType:{type:'INT64'},  parameterValue:{value:String(CONFIG.RANK_TOPN)}}
    ];
    let recs = bq.query(sqlNearest, paramsNearest, true, 600);

    if (!recs || !recs.length){
      const sqlAny = [
        'WITH nearest AS (',
        '  SELECT slot_dt_local',
        '  FROM '+baseTbl,
        '  WHERE username_page=@up',
        '  ORDER BY slot_dt_local ASC',
        '  LIMIT 1',
        ')',
        'SELECT caption_id, SUBSTR(COALESCE(caption_text, \'\'), 1, 500) AS caption_text,',
        '       p_buy_eb, rps_eb_price, se_bonus, style_score,',
        '       (rps_eb_price + se_bonus + COALESCE(style_score,0)) AS score_final,',
        '       rn',
        'FROM '+baseTbl,
        'WHERE username_page=@up',
        '  AND slot_dt_local = (SELECT slot_dt_local FROM nearest)',
        'ORDER BY rn',
        'LIMIT @topn'
      ].join('\n');
      recs = bq.query(sqlAny, [
        {name:'up',   parameterType:{type:'STRING'}, parameterValue:{value:up}},
        {name:'topn', parameterType:{type:'INT64'},  parameterValue:{value:String(CONFIG.RANK_TOPN)}}
      ], true, 600);
    }

    if (!recs || !recs.length){
      SpreadsheetApp.getUi().alert('No caption candidates available for this slot/page.\n(If this is far in future, next-24h table may not cover it yet.)');
      return;
    }

    // HTML modal
    const nf = function(x,d){ var n=Number(x||0); return isFinite(n)? n.toFixed(d) : '0'; };
    var html = [];
    html.push('<!doctype html><html><head><meta charset="utf-8"/>',
      '<style>body{font-family:Arial,sans-serif;padding:20px;margin:0}.hdr{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;padding:14px;margin:-20px -20px 14px -20px}.item{border:2px solid #e5e7eb;border-radius:12px;padding:12px;margin:10px 0;cursor:pointer;transition:.2s;background:#fff;position:relative}.item:hover{border-color:#667eea;box-shadow:0 4px 14px rgba(102,126,234,.2)}.sel{background:#e0e7ff;border-color:#667eea}.id{font-weight:bold;color:#667eea}.meta{display:flex;gap:16px;color:#6b7280;font-size:12px;margin-top:6px;flex-wrap:wrap}.score{position:absolute;top:8px;right:8px;background:#667eea;color:#fff;border-radius:16px;padding:4px 8px;font-weight:bold;font-size:12px}.btns{display:flex;justify-content:flex-end;gap:8px;margin-top:12px}.btn{padding:10px 16px;border:none;border-radius:8px;font-weight:bold;cursor:pointer}.p{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff}.s{background:#e5e7eb;color:#374151}</style>',
      '</head><body>');
    html.push('<div class="hdr"><b>Top captions</b> for <b>',model,'</b> · <b>',page,'</b> @ <b>',hod,':00</b></div><div id="list">');

    for (var i=0;i<recs.length;i++){
      var r=recs[i], score=nf(r.score_final,4);
      var meta = [
        'RN #'+(r.rn||i+1),
        'p='+nf(r.p_buy_eb,6),
        'RPS='+nf(r.rps_eb_price,5),
        'UCB='+nf(r.se_bonus,5),
        'Style='+nf(r.style_score,4)
      ].join(' · ');
      html.push(
        '<div class="item" data-id="', (r.caption_id||''), '" data-r="', (r.rn||i+1),
        '" data-s="', score ,'">',
        '<div class="score">',score,'</div>',
        '<div class="id">Caption ID: ',(r.caption_id||''),'</div>',
        '<div>', String(r.caption_text||'').replace(/</g,'&lt;').replace(/>/g,'&gt;') ,'</div>',
        '<div class="meta">', meta ,'</div>',
        '</div>'
      );
    }

    html.push('</div>',
      '<div class="btns"><button class="btn s" onclick="google.script.host.close()">Cancel</button>',
      '<button class="btn p" onclick="pick()">Use selected</button></div>',
      '<script>',
      'var sel=null;',
      'document.querySelectorAll(".item").forEach(function(x){x.addEventListener("click",function(){document.querySelectorAll(".item").forEach(function(y){y.classList.remove("sel")});this.classList.add("sel");sel=this;});});',
      'function pick(){if(!sel){alert("Select a caption");return;}var id=sel.getAttribute("data-id"), rnk=parseInt(sel.getAttribute("data-r"),10)||null, sc=parseFloat(sel.getAttribute("data-s"))||null, txt=sel.querySelector(".id").nextElementSibling.textContent;google.script.run.withSuccessHandler(function(){google.script.host.close();}).withFailureHandler(function(e){alert("Error: "+e);}).applyCaptionChoice(',row,',"',name,'",id,txt,rnk,sc);} ',
      '</script>',
      '</body></html>'
    );

    SpreadsheetApp.getUi().showModalDialog(HtmlService.createHtmlOutput(html.join('')).setWidth(900).setHeight(600), 'Pick caption — '+model+' · '+page+' @ '+hod+':00');
  }catch(e){ handleError(e,'showCaptionPicker'); }
}

function applyCaptionChoice(row, sheetName, captionId, captionText /*, recoRank, recoConfidence */){
  try{
    const ss = SpreadsheetApp.getActiveSpreadsheet(); const sh = ss.getSheetByName(sheetName); const tz = Session.getScriptTimeZone();
    if (sheetName===CONFIG.SHEETS.MY_DAY){
      sh.getRange(row,IDX.DAY.CAPTION_ID).setValue(captionId);
      sh.getRange(row,IDX.DAY.CAPTION_PREV).setValue(String(captionText||'').substring(0,200));
      sh.getRange(row,IDX.DAY.CAPTION_ID,1,2).setBackground(CONFIG.COLORS.CAPTION_SELECTED);
      const trackingHash = sh.getRange(row,IDX.DAY.HASH ).getValue();
      const up           = sh.getRange(row,IDX.DAY.UPAGE).getValue();
      const model        = sh.getRange(row,IDX.DAY.MODEL).getValue();
      const page         = sh.getRange(row,IDX.DAY.PAGE ).getValue();
      const hod          = Number(sh.getRange(row,IDX.DAY.HOD  ).getValue()) || parseInt(String(sh.getRange(row,IDX.DAY.TIME).getValue()).split(':')[0],10) || 0;
      const price        = Number(sh.getRange(row,IDX.DAY.PRICE).getValue()) || 0;
      const dateISO      = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');
      bqInsertSend_(trackingHash, model, page, up, getSchedulerCode_(), getCurrentUserEmail_(), dateISO, hod, price, captionId, 'Planned', 'caption_selected');
    }else{
      sh.getRange(row,IDX.WEEK.CAPTION_ID).setValue(captionId);
      sh.getRange(row,IDX.WEEK.CAPTION_PREV).setValue(String(captionText||'').substring(0,200));
      sh.getRange(row,IDX.WEEK.CAPTION_ID,1,2).setBackground(CONFIG.COLORS.CAPTION_SELECTED);
      const dateISO      = Utilities.formatDate(new Date(sh.getRange(row,IDX.WEEK.DATE).getValue()), tz, 'yyyy-MM-dd');
      const recTime      = String(sh.getRange(row,IDX.WEEK.REC_TIME).getValue()||'');
      const actTime      = String(sh.getRange(row,IDX.WEEK.ACT_TIME).getValue()||'');
      const hod          = parseInt((actTime||recTime).split(':')[0],10) || 0;
      const price        = Number(sh.getRange(row,IDX.WEEK.ACT_PRICE).getValue() || sh.getRange(row,IDX.WEEK.REC_PRICE).getValue() || 0);
      const trackingHash = sh.getRange(row,IDX.WEEK.HASH ).getValue();
      const up           = sh.getRange(row,IDX.WEEK.UPAGE).getValue();
      const model        = sh.getRange(row,IDX.WEEK.MODEL).getValue();
      const page         = sh.getRange(row,IDX.WEEK.PAGE).getValue();
      bqInsertSend_(trackingHash, model, page, up, getSchedulerCode_(), getCurrentUserEmail_(), dateISO, hod, price, captionId, 'Planned', 'caption_selected');
    }
    SpreadsheetApp.getUi().alert('✅ Caption '+captionId+' selected');
  }catch(e){ handleError(e,'applyCaptionChoice'); }
}

function getSchedulerCode_(){ try{ const s = new RecommendationEngine().getSchedulerInfo(); return s? s.scheduler_name : ''; }catch(_){ return ''; } }

// ==================== Logging & Sync ====================
function submitDailyPlan(){
  try{
    const ss = SpreadsheetApp.getActiveSpreadsheet(); const day = ss.getSheetByName(CONFIG.SHEETS.MY_DAY);
    if (!day) throw new Error('My Day sheet not found');
    const data = day.getDataRange().getValues(); const sched = new RecommendationEngine().getSchedulerInfo();
    if (!sched){ SpreadsheetApp.getUi().alert('Your email is not registered as a scheduler.'); return; }

    const todayISO = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    var sent=0, ready=0;
    for (var i=1;i<data.length;i++){
      var r=data[i]; if (!r[IDX.DAY.TIME-1]) continue;
      var st=String(r[IDX.DAY.STATUS-1]||''); if (st!=='Ready' && st!=='Sent') continue;
      var hash=r[IDX.DAY.HASH-1]||null, up=r[IDX.DAY.UPAGE-1]||(r[IDX.DAY.MODEL-1]+'__'+r[IDX.DAY.PAGE-1]);
      var hod=Number(r[IDX.DAY.HOD-1])||parseInt(String(r[IDX.DAY.TIME-1]).split(':')[0],10); var price=Number(r[IDX.DAY.PRICE-1])||0; var cap=String(r[IDX.DAY.CAPTION_ID-1]||''); var act=(st==='Sent')?'sent':'ready';
      bqInsertSend_(hash, String(r[IDX.DAY.MODEL-1]), String(r[IDX.DAY.PAGE-1]), up, sched.scheduler_name, getCurrentUserEmail_(), todayISO, hod, price, cap, st, act);
      if (act==='sent') sent++; else ready++;
    }
    SpreadsheetApp.getUi().alert('Submitted '+sent+' Sent and '+ready+' Ready rows to BigQuery');
  }catch(e){ handleError(e,'submitDailyPlan'); }
}

function bqInsertSend_(trackingHash, usernameStd, pageType, usernamePage, schedulerCode, schedulerEmail, dateISO, hodLocal, priceUsd, captionId, status, action){
  const bq = new BigQueryService();
  const sql = [
    'INSERT `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_OPS+'.send_log`',
    '  (action_ts, action_date, tracking_hash, username_std, page_type, username_page,',
    '   scheduler_code, scheduler_email, date_local, hod_local, price_usd,',
    '   caption_id, status, action, source)',
    'SELECT',
    "  CURRENT_TIMESTAMP(), CURRENT_DATE(), @th, @u, @pt, @up,",
    "  @sc, @se, @dt, @hh, @pr, NULLIF(@cap,''), @st, @ac, 'sheets_v7_1'",
    'WHERE NOT EXISTS (',
    '  SELECT 1 FROM `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_OPS+'.send_log`',
    '  WHERE tracking_hash=@th AND action=@ac',
    "    AND (@ac!='caption_selected' OR COALESCE(caption_id,'')=COALESCE(@cap,''))",
    ')'
  ].join('\n');
  const params = [
    {name:'th',  parameterType:{type:'STRING'},  parameterValue:{value:String(trackingHash||'')}},
    {name:'u',   parameterType:{type:'STRING'},  parameterValue:{value:String(usernameStd)}},
    {name:'pt',  parameterType:{type:'STRING'},  parameterValue:{value:String(pageType)}},
    {name:'up',  parameterType:{type:'STRING'},  parameterValue:{value:String(usernamePage)}},
    {name:'sc',  parameterType:{type:'STRING'},  parameterValue:{value:String(schedulerCode)}},
    {name:'se',  parameterType:{type:'STRING'},  parameterValue:{value:String(schedulerEmail)}},
    {name:'dt',  parameterType:{type:'DATE'},    parameterValue:{value:String(dateISO)}},
    {name:'hh',  parameterType:{type:'INT64'},   parameterValue:{value:String(hodLocal)}},
    {name:'pr',  parameterType:{type:'NUMERIC'}, parameterValue:{value:String(priceUsd)}},
    {name:'cap', parameterType:{type:'STRING'},  parameterValue:{value:String(captionId||'')}},
    {name:'st',  parameterType:{type:'STRING'},  parameterValue:{value:String(status)}},
    {name:'ac',  parameterType:{type:'STRING'},  parameterValue:{value:String(action)}}
  ];
  try{ bq.exec(sql, params); }catch(e){ console.error('bqInsertSend_', e); }
}

function syncWithBigQuery(){ try{ refreshWeekPlan(); generateBrief(); checkAlerts(); SpreadsheetApp.getUi().alert('Sync complete!'); }catch(e){ handleError(e,'syncWithBigQuery'); } }

// ==================== Installable onEdit ====================
function onEdit_Installable(e){
  try{
    const sh=e.range.getSheet(); const name=sh.getName(); if ([CONFIG.SHEETS.WEEK_PLAN, CONFIG.SHEETS.MY_DAY].indexOf(name)===-1) return; const row=e.range.getRow(); if (row===1) return; const col=e.range.getColumn();
    const val=String(e.range.getValue()||''); const isWeek=(name===CONFIG.SHEETS.WEEK_PLAN && col===IDX.WEEK.STATUS); const isDay=(name===CONFIG.SHEETS.MY_DAY && col===IDX.DAY.STATUS);
    if (!(isWeek||isDay)) return; if (['Sent','Ready'].indexOf(val)===-1) return; const sched=new RecommendationEngine().getSchedulerInfo(); if (!sched) return;

    if (isWeek){
      const tz=Session.getScriptTimeZone(); const dateISO=Utilities.formatDate(new Date(sh.getRange(row,IDX.WEEK.DATE).getValue()), tz, 'yyyy-MM-dd');
      const recTime=String(sh.getRange(row,IDX.WEEK.REC_TIME).getValue()||''); const actTime=String(sh.getRange(row,IDX.WEEK.ACT_TIME).getValue()||''); const hod=parseInt((actTime||recTime).split(':')[0],10);
      const price=Number(sh.getRange(row,IDX.WEEK.ACT_PRICE).getValue()||sh.getRange(row,IDX.WEEK.REC_PRICE).getValue()||0); const cap=String(sh.getRange(row,IDX.WEEK.CAPTION_ID).getValue()||'');
      const hash=sh.getRange(row,IDX.WEEK.HASH).getValue(); const up=sh.getRange(row,IDX.WEEK.UPAGE).getValue(); const model=String(sh.getRange(row,IDX.WEEK.MODEL).getValue()); const page=String(sh.getRange(row,IDX.WEEK.PAGE).getValue());
      const act=(val==='Sent')?'sent':'ready'; bqInsertSend_(hash, model, page, up, sched.scheduler_name, getCurrentUserEmail_(), dateISO, hod, price, cap, val, act); return;
    }
    if (isDay){
      const tz=Session.getScriptTimeZone(); const dateISO=Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');
      const hod=Number(sh.getRange(row,IDX.DAY.HOD).getValue()) || parseInt(String(sh.getRange(row,IDX.DAY.TIME).getValue()).split(':')[0],10);
      const price=Number(sh.getRange(row,IDX.DAY.PRICE).getValue()||0); const cap=String(sh.getRange(row,IDX.DAY.CAPTION_ID).getValue()||'');
      const hash=sh.getRange(row,IDX.DAY.HASH).getValue(); const up=sh.getRange(row,IDX.DAY.UPAGE).getValue(); const model=String(sh.getRange(row,IDX.DAY.MODEL).getValue()); const page=String(sh.getRange(row,IDX.DAY.PAGE).getValue());
      const act=(val==='Sent')?'sent':'ready'; bqInsertSend_(hash, model, page, up, sched.scheduler_name, getCurrentUserEmail_(), dateISO, hod, price, cap, val, act); return;
    }
  }catch(err){ handleError(err,'onEdit_Installable'); }
}
function onEdit(e){ return; }

// ==================== Triggers ====================
function installTriggers(){
  try{
    const ssId=SpreadsheetApp.getActive().getId();
    ScriptApp.getProjectTriggers().forEach(function(t){ var f=t.getHandlerFunction(); if (['autoRefreshWeekPlan','dailyBriefUpdate','onEdit_Installable'].indexOf(f)!==-1) ScriptApp.deleteTrigger(t); });
    ScriptApp.newTrigger('autoRefreshWeekPlan').timeBased().everyHours(4).create();
    ScriptApp.newTrigger('dailyBriefUpdate').timeBased().atHour(6).everyDays(1).create();
    ScriptApp.newTrigger('onEdit_Installable').forSpreadsheet(ssId).onEdit().create();
  }catch(e){ console.error('installTriggers', e); }
}
function autoRefreshWeekPlan(){ try{ refreshWeekPlan(); generateBrief(); checkAlerts(); }catch(e){ handleError(e,'autoRefreshWeekPlan'); } }
function dailyBriefUpdate(){ try{ generateBrief(); checkAlerts(); }catch(e){ handleError(e,'dailyBriefUpdate'); } }

// ==================== Brief & Alerts ====================
function generateBrief(){
  try{
    const ss=SpreadsheetApp.getActiveSpreadsheet(); const sheet=ss.getSheetByName(CONFIG.SHEETS.BRIEF); if (!sheet) throw new Error('Brief sheet not found');
    const sched=new RecommendationEngine().getSchedulerInfo(); if (!sched){ SpreadsheetApp.getUi().alert('Your email is not registered as a scheduler.'); return; }
    const sql = [
      'SELECT username_std AS model,',
      "       'main' AS page_type,",
      "       COALESCE(page_state,'balanced') AS state,",
      '       COALESCE(active_fans,0) AS fans,',
      '       COALESCE(rev_7d,0) AS revenue_7d,',
      "       CONCAT(hod1, ', ', hod2, ', ', hod3, ', ', hod4, ', ', hod5) AS best_hours,",
      "       CONCAT('$', CAST(price_p25 AS STRING), ' - $', CAST(price_p75 AS STRING)) AS price_band,",
      "       'See Assistant' AS top_captions,",
      '       0 AS avoid_count',
      'FROM `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_SHEETS+'.v_daily_brief_user_flat`'
    ].join('\n');
    const brief = new BigQueryService().query(sql, [], false, CONFIG.CACHE_TTL.BRIEF);
    const lr=sheet.getLastRow(); if (lr>1){ sheet.getRange(2,1, lr-1, sheet.getLastColumn()).clear({contentsOnly:true}); }
    if (brief.length){ const rows = brief.map(function(b){ return [ b.model, 'main', b.state, Number(b.fans)||0, Number(b.revenue_7d)||0, b.best_hours||'', b.price_band||'', b.top_captions||'', 0 ]; }); sheet.getRange(2,1,rows.length,rows[0].length).setValues(rows); sheet.getRange(2,5,rows.length,1).setNumberFormat('$#,##0'); }
    SpreadsheetApp.getUi().alert('Brief generated for '+brief.length+' models');
  }catch(e){ handleError(e,'generateBrief'); }
}

function checkAlerts(){
  try{
    const ss=SpreadsheetApp.getActiveSpreadsheet(); const sheet=ss.getSheetByName(CONFIG.SHEETS.ALERTS); if (!sheet) throw new Error('Alerts sheet not found');
    const sql = [
      "SELECT 'MEDIUM' AS priority, 'Schedule Check' AS type,",
      '       t.username_std AS model, t.page_type,',
      "       CONCAT('Check schedule for ', CAST(a.date_local AS STRING)) AS message,",
      "       'Review timing' AS action_required",
      'FROM `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_MART+'.v_weekly_feasibility_alerts` a',
      // Use CORE weekly pages final
      'JOIN `'+CONFIG.PROJECT_ID+'.'+CONFIG.DATASET_CORE+'.v_weekly_template_7d_pages_final` t',
      '  ON t.username_std=a.username_std AND t.date_local=a.date_local',
      'LIMIT 50'
    ].join('\n');
    const alerts = new BigQueryService().query(sql, [], false);
    const lr=sheet.getLastRow(); if (lr>1){ sheet.getRange(2,1, lr-1, sheet.getLastColumn()).clear({contentsOnly:true}); }
    if (alerts.length){ const rows = alerts.map(function(a){ return [a.priority,a.type,a.model,a.page_type,a.message,a.action_required]; }); sheet.getRange(2,1,rows.length,rows[0].length).setValues(rows); for (var i=0;i<rows.length;i++){ var color = rows[i][0]==='HIGH'?CONFIG.COLORS.ALERT: rows[i][0]==='MEDIUM'?CONFIG.COLORS.MODIFIED: CONFIG.COLORS.LOCKED; sheet.getRange(i+2,1,1,6).setBackground(color); } }
  }catch(e){ handleError(e,'checkAlerts'); }
}

// ==================== Utilities ====================
function viewActivityLog(){ try{ const ss=SpreadsheetApp.getActiveSpreadsheet(); const sheet=ss.getSheetByName(CONFIG.SHEETS.LOG); if (sheet) SpreadsheetApp.setActiveSheet(sheet); else SpreadsheetApp.getUi().alert('Activity Log sheet not found'); }catch(e){ handleError(e,'viewActivityLog'); } }
function openSettings(){ try{ const ss=SpreadsheetApp.getActiveSpreadsheet(); const sheet=ss.getSheetByName(CONFIG.SHEETS.SETTINGS)||ss.insertSheet(CONFIG.SHEETS.SETTINGS); sheet.clear(); sheet.getRange(1,1).setValue('EROS Scheduler Hub - Settings').setFontSize(16).setFontWeight('bold'); const rows=[ ['Setting','Current Value','New Value'], ['Cooldown Days', CONFIG.COOLDOWN_DAYS,''], ['User Cache TTL', CONFIG.CACHE_TTL.USER,''], ['Reco Cache TTL', CONFIG.CACHE_TTL.RECO,''] ]; sheet.getRange(3,1,rows.length,3).setValues(rows); sheet.getRange(3,1,1,3).setBackground(CONFIG.COLORS.HEADER).setFontColor('#fff').setFontWeight('bold'); SpreadsheetApp.setActiveSheet(sheet);}catch(e){ handleError(e,'openSettings'); } }
function showHelp(){ SpreadsheetApp.getUi().alert('Scheduler Hub','Use 📅/✅ to load schedules. Use 🧠 to pick a caption (reads precomputed Top-N per slot). Submissions log to ops.send_log.', SpreadsheetApp.getUi().ButtonSet.OK); }

// ==================== Error handling & Install ====================
function handleError(err, ctx){
  console.error('Error in '+ctx+':', err);
  try{
    const ss=SpreadsheetApp.getActiveSpreadsheet(); const log=ss.getSheetByName(CONFIG.SHEETS.LOG);
    if (log){ log.appendRow([new Date().toISOString(), getCurrentUserEmail_(), 'error', '', '', JSON.stringify({context:ctx, msg:(err&&err.message)||String(err)}), 'ERROR']); }
  }catch(_){}
  try{ SpreadsheetApp.getUi().alert('Error','An error occurred in '+ctx+'\\n\\nDetails: '+((err&&err.message)||String(err)), SpreadsheetApp.getUi().ButtonSet.OK); }catch(_){}
}
function onInstall(e){ onOpen(e); }
function authorizeOnce(){ const ss=SpreadsheetApp.getActiveSpreadsheet(); ss.getId(); BigQuery.Jobs.query({query:'SELECT 1',useLegacySql:false}, CONFIG.PROJECT_ID); ScriptApp.getProjectTriggers(); }
function checkMyEmail(){ SpreadsheetApp.getUi().alert('Your email','Script sees you as: '+getCurrentUserEmail_(), SpreadsheetApp.getUi().ButtonSet.OK); }