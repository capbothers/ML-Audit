/**
 * Google Ads Script — Export Campaign + Product Data to Google Sheets
 * (MCC / Manager Account version)
 *
 * Exports two tabs:
 *   "Campaign Data"  — campaign-level daily metrics
 *   "Product Data"   — product/SKU-level daily metrics (Shopping & PMax)
 *
 * ONE-TIME SETUP:
 * 1. Create a Google Sheet and share it with the service account email
 * 2. Paste this script into Google Ads > Tools > Scripts (at MCC level)
 * 3. Update SPREADSHEET_ID below
 * 4. Authorize and Run
 * 5. Schedule daily
 *
 * Our system reads from this Sheet via: POST /sync/google-ads/import-from-sheet
 */

// ── CONFIGURATION ────────────────────────────────────────────────
var SPREADSHEET_ID = '17dITR8FPHYytYF6HF8Z6D2aMHMvPojx8oHm1V4JmQxk';
var CAMPAIGN_TAB = 'Campaign Data';
var PRODUCT_TAB = 'Product Data';
var LOOKBACK_DAYS = 30;  // How many days of campaign data to export each run
var PRODUCT_LOOKBACK_DAYS = 120;  // Product data uses 4 months (aggregated, not daily)
// ── END CONFIGURATION ────────────────────────────────────────────

function main() {
  var spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);

  var allCampaignRows = [];
  var allProductRows = [];
  var accountNames = [];

  // Iterate over all client accounts under this MCC
  var accountIterator = MccApp.accounts().get();

  while (accountIterator.hasNext()) {
    var account = accountIterator.next();
    MccApp.select(account);

    var accountName = account.getName();
    accountNames.push(accountName);
    Logger.log('Processing account: ' + accountName + ' (' + account.getCustomerId() + ')');

    try {
      var campaignRows = exportCampaignData();
      Logger.log('  Campaigns: ' + campaignRows.length + ' rows');
      allCampaignRows = allCampaignRows.concat(campaignRows);
    } catch (e) {
      Logger.log('  Campaign ERROR: ' + e.message);
    }

    try {
      var productRows = exportProductData();
      Logger.log('  Products: ' + productRows.length + ' rows');
      allProductRows = allProductRows.concat(productRows);
    } catch (e) {
      Logger.log('  Product ERROR: ' + e.message);
    }
  }

  // ── Write Campaign Data tab ──
  var campaignHeaders = [
    'Date', 'Campaign ID', 'Campaign Name', 'Campaign Type', 'Campaign Status',
    'Impressions', 'Clicks', 'Cost', 'Conversions', 'Conv. Value',
    'CTR', 'Avg. CPC', 'Conv. Rate',
    'Search Impr. Share', 'Search Lost IS (Budget)', 'Search Lost IS (Rank)'
  ];
  writeTab(spreadsheet, CAMPAIGN_TAB, campaignHeaders, allCampaignRows);

  // ── Write Product Data tab (aggregated — one row per product per campaign) ──
  var productHeaders = [
    'Product ID', 'Product Title', 'Campaign ID', 'Campaign Name',
    'Impressions', 'Clicks', 'Cost', 'Conversions', 'Conv. Value',
    'Period Start', 'Period End'
  ];
  writeTab(spreadsheet, PRODUCT_TAB, productHeaders, allProductRows);

  // ── Metadata ──
  var metaSheet = spreadsheet.getSheetByName('_metadata');
  if (!metaSheet) {
    metaSheet = spreadsheet.insertSheet('_metadata');
  }
  metaSheet.clear();
  metaSheet.getRange(1, 1, 6, 2).setValues([
    ['Last Updated', new Date().toISOString()],
    ['Accounts', accountNames.join(', ')],
    ['Campaign Rows', allCampaignRows.length],
    ['Product Rows', allProductRows.length],
    ['Lookback Days', LOOKBACK_DAYS],
    ['Total Rows', allCampaignRows.length + allProductRows.length]
  ]);

  Logger.log('Total: ' + allCampaignRows.length + ' campaign rows, ' +
             allProductRows.length + ' product rows from ' +
             accountNames.length + ' accounts');
}


function writeTab(spreadsheet, tabName, headers, rows) {
  var sheet = spreadsheet.getSheetByName(tabName);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(tabName);
  }
  sheet.clear();
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold');
  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
  }
}


function exportCampaignData() {
  var today = new Date();
  var startDate = new Date();
  startDate.setDate(today.getDate() - LOOKBACK_DAYS);

  var query = 'SELECT ' +
    'segments.date, ' +
    'campaign.id, ' +
    'campaign.name, ' +
    'campaign.advertising_channel_type, ' +
    'campaign.status, ' +
    'metrics.impressions, ' +
    'metrics.clicks, ' +
    'metrics.cost_micros, ' +
    'metrics.conversions, ' +
    'metrics.conversions_value, ' +
    'metrics.ctr, ' +
    'metrics.average_cpc, ' +
    'metrics.conversions_from_interactions_rate, ' +
    'metrics.search_impression_share, ' +
    'metrics.search_budget_lost_impression_share, ' +
    'metrics.search_rank_lost_impression_share ' +
    'FROM campaign ' +
    'WHERE segments.date BETWEEN "' + formatDateForGaql(startDate) + '" ' +
    'AND "' + formatDateForGaql(today) + '" ' +
    'ORDER BY segments.date DESC';

  var rows = [];
  var report = AdsApp.search(query);

  while (report.hasNext()) {
    var row = report.next();
    var costMicros = row.metrics.costMicros || 0;
    var avgCpcMicros = row.metrics.averageCpc || 0;

    rows.push([
      row.segments.date,
      row.campaign.id,
      row.campaign.name,
      row.campaign.advertisingChannelType || '',
      row.campaign.status || '',
      row.metrics.impressions || 0,
      row.metrics.clicks || 0,
      (costMicros / 1000000).toFixed(2),
      (row.metrics.conversions || 0).toFixed(2),
      (row.metrics.conversionsValue || 0).toFixed(2),
      ((row.metrics.ctr || 0) * 100).toFixed(2) + '%',
      (avgCpcMicros / 1000000).toFixed(2),
      ((row.metrics.conversionsFromInteractionsRate || 0) * 100).toFixed(2) + '%',
      formatPercent(row.metrics.searchImpressionShare),
      formatPercent(row.metrics.searchBudgetLostImpressionShare),
      formatPercent(row.metrics.searchRankLostImpressionShare)
    ]);
  }

  return rows;
}


function exportProductData() {
  var today = new Date();
  var startDate = new Date();
  startDate.setDate(today.getDate() - PRODUCT_LOOKBACK_DAYS);

  var startStr = formatDateForGaql(startDate);
  var endStr = formatDateForGaql(today);

  // Query includes segments.date (required by shopping_performance_view)
  // but we aggregate by product+campaign in JS before writing to the sheet.
  var query = 'SELECT ' +
    'segments.date, ' +
    'segments.product_item_id, ' +
    'segments.product_title, ' +
    'campaign.id, ' +
    'campaign.name, ' +
    'metrics.impressions, ' +
    'metrics.clicks, ' +
    'metrics.cost_micros, ' +
    'metrics.conversions, ' +
    'metrics.conversions_value ' +
    'FROM shopping_performance_view ' +
    'WHERE segments.date BETWEEN "' + startStr + '" ' +
    'AND "' + endStr + '" ' +
    'ORDER BY metrics.cost_micros DESC';

  // Aggregate daily rows into one row per product per campaign
  var agg = {};  // key: "productId|campaignId"
  var report = AdsApp.search(query);
  var dailyRows = 0;

  while (report.hasNext()) {
    var row = report.next();
    dailyRows++;
    var pid = row.segments.productItemId || '';
    var cid = row.campaign.id;
    var key = pid + '|' + cid;

    if (!agg[key]) {
      agg[key] = {
        productItemId: pid,
        productTitle: row.segments.productTitle || '',
        campaignId: cid,
        campaignName: row.campaign.name,
        impressions: 0,
        clicks: 0,
        costMicros: 0,
        conversions: 0,
        conversionsValue: 0
      };
    }

    agg[key].impressions += Number(row.metrics.impressions || 0);
    agg[key].clicks += Number(row.metrics.clicks || 0);
    agg[key].costMicros += Number(row.metrics.costMicros || 0);
    agg[key].conversions += Number(row.metrics.conversions || 0);
    agg[key].conversionsValue += Number(row.metrics.conversionsValue || 0);
  }

  Logger.log('  Product daily rows: ' + dailyRows + ' → aggregated: ' + Object.keys(agg).length);

  // Convert aggregated map to rows
  var rows = [];
  var keys = Object.keys(agg);
  for (var i = 0; i < keys.length; i++) {
    var p = agg[keys[i]];
    rows.push([
      p.productItemId,
      p.productTitle,
      p.campaignId,
      p.campaignName,
      p.impressions,
      p.clicks,
      (p.costMicros / 1000000).toFixed(2),
      p.conversions.toFixed(2),
      p.conversionsValue.toFixed(2),
      startStr,
      endStr
    ]);
  }

  // Sort by cost descending
  rows.sort(function(a, b) { return parseFloat(b[6]) - parseFloat(a[6]); });

  return rows;
}


function formatDateForGaql(date) {
  var year = date.getFullYear();
  var month = ('0' + (date.getMonth() + 1)).slice(-2);
  var day = ('0' + date.getDate()).slice(-2);
  return year + '-' + month + '-' + day;
}


function formatPercent(value) {
  if (value === '' || value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'number') {
    return (value * 100).toFixed(2) + '%';
  }
  return String(value);
}
