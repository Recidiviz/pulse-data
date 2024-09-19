// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================
/* File containing functions that construct SQL Queries used in CreateReport.gs. */

/**
 * Get start date
 * Query the Big Query database for the start_date.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} timePeriod The time period passed in from the Google Form (ex: 'MONTH', 'QUARTER', or 'YEAR')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 **/
function getStartDate(stateCode, timePeriod, endDateString) {
  const queryString = `
    SELECT start_date
    FROM aggregated_metrics.supervision_state_aggregated_metrics_materialized
    WHERE state_code = '${stateCode}'
    AND period = '${timePeriod}'
    AND end_date = '${endDateString}'`;

  const queryOutput = runQuery(queryString)[0];
  const queryOutputLength = queryOutput.length;
  if (queryOutputLength !== 1) {
    throw new Error(
      `Expected 1 row in query output, but got length: ${queryOutputLength}. Query output: ${queryOutput}.`
    );
  }

  const splitStartDate = queryOutput[0].split("-");
  const startDate = `${splitStartDate[1]}/${splitStartDate[2]}/${splitStartDate[0]}`;
  Logger.log("startDate: %s", startDate);

  return startDate;
}

/**
 * Construct usage and impact text
 * Given parameters provided by the user, constructs a query string and call RunQuery
 * to query the BiqQuery database. After fetching the total number of individuals surfaced, eligible, reviewed, and ineligible, adds them and returns the integer as a formatted
 * string.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @returns {map} An object that contains the number of jii almostEligible, eligible, markedIneligible, eligibleAndViewed and eligibleAndNotViewed
 **/
function constructUsageAndImpactText(
  stateCode,
  endDateString,
  completionEventType
) {
  const almostEligibleColumn = `avg_daily_population_task_almost_eligible_${completionEventType.toLowerCase()}`;
  const eligibleColumn = `avg_population_task_eligible_${completionEventType.toLowerCase()}`;
  const markedIneligibleColumn = `avg_daily_population_task_marked_ineligible_${completionEventType.toLowerCase()}`;
  const eligibleAndViewedColumn = `avg_daily_population_task_eligible_and_viewed_${completionEventType.toLowerCase()}`;
  const eligibleAndNotViewedColumn = `avg_daily_population_task_eligible_and_not_viewed_${completionEventType.toLowerCase()}`;

  const usageTable = `justice_involved_state_day_aggregated_metrics_materialized`;

  const queryString = `
    SELECT
    ${almostEligibleColumn},
    ${eligibleColumn},
    ${markedIneligibleColumn},
    ${eligibleAndViewedColumn},
    ${eligibleAndNotViewedColumn}
    FROM \`impact_reports.${usageTable}\`
    WHERE state_code = '${stateCode}'
    AND end_date = '${endDateString}'`;

  const queryOutput = runQuery(queryString)[0];
  const queryOutputLength = queryOutput.length;
  if (queryOutputLength !== 5) {
    throw new Error(
      `Expected 5 columns in query output, but got length: ${queryOutputLength}. Query output: ${queryOutput}.`
    );
  }

  const almostEligible = parseInt(queryOutput[0]);
  const eligible = parseInt(queryOutput[1]);
  const markedIneligible = parseInt(queryOutput[2]);
  const eligibleAndViewed = parseInt(queryOutput[3]);
  const eligibleAndNotViewed = parseInt(queryOutput[4]);

  Logger.log("Almost Eligible: %s", almostEligible);
  Logger.log("Eligible: %s", eligible);
  Logger.log("Marked Ineligible: %s", markedIneligible);
  Logger.log("Eligible and Viewed: %s", eligibleAndViewed);
  Logger.log("Eligible and Not Viewed: %s", eligibleAndNotViewed);

  return {
    almostEligible: almostEligible,
    eligible: eligible,
    markedIneligible: markedIneligible,
    eligibleAndViewed: eligibleAndViewed,
    eligibleAndNotViewed: eligibleAndNotViewed,
  };
}

/**
 * Construct opportunities granted text
 * Given parameters provided by the user, constructs a query string and call RunQuery
 * to query the BiqQuery database. After fetching the total number of supervision and
 * facilities opportunities granted, adds them and returns the integer as a formatted
 * string.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} timePeriod The time period passed in from the Google Form (ex: 'MONTH', 'QUARTER', or 'YEAR')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @param {string} system The system of the workflow (ex: 'SUPERVISION' or 'INCARCERATION')
 * @returns {number} opportunitiesGranted The number of opportunities granted for the given workflow
 **/
function constructOpportunitiesGrantedText(
  stateCode,
  timePeriod,
  endDateString,
  completionEventType,
  system
) {
  const opportunityColumn = `task_completions_${completionEventType.toLowerCase()}`;
  const opportunityTable = `${system.toLowerCase()}_state_aggregated_metrics_materialized`;

  const queryString = `
    SELECT ${opportunityColumn}
    FROM \`aggregated_metrics.${opportunityTable}\`
    WHERE state_code = '${stateCode}'
    AND period = '${timePeriod}'
    AND end_date = '${endDateString}'`;

  const queryOutput = runQuery(queryString)[0];
  const queryOutputLength = queryOutput.length;
  if (queryOutputLength !== 1) {
    throw new Error(
      `Expected 1 row in query output, but got length: ${queryOutputLength}. Query output: ${queryOutput}.`
    );
  }

  const opportunitiesGranted = parseInt(queryOutput[0]);
  Logger.log("Opportunities Granted: %s", opportunitiesGranted);

  return opportunitiesGranted;
}

/**
 * Get max region
 * Given an array of arrays that contain the number of opportunities granted for each region, return the region with the max number of opportunities granted.
 * @param {array} supervisionDistrictData
 * @returns {string} the region with with max number of opportunities granted
 **/
function getMaxRegion(supervisionDistrictData) {
  var maxRegion = null;
  var maxNum = 0;
  supervisionDistrictData.forEach((arr) => {
    if (parseFloat(arr[1]) > maxNum) {
      maxNum = arr[1];
      maxRegion = arr[0];
    }
  });

  return maxRegion;
}

/**
 * Get supervision district data
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BiqQuery database.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} timePeriod The time period passed in from the Google Form (ex: 'MONTH', 'QUARTER', or 'YEAR')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @param {string} system The system of the workflow (ex: 'SUPERVISION' or 'INCARCERATION')
 * @returns {object} supervisionDistrictData An array or arrays containing data for each district/facility. Also returns the xAxisColumn and yAxisColumn.
 */
function getSupervisionDistrictData(
  stateCode,
  timePeriod,
  endDateString,
  completionEventType,
  system
) {
  var xAxisColumn = "";
  var yAxisColumn = "";
  var tableName = "";

  if (system === "SUPERVISION") {
    xAxisColumn = "district_name";
    tableName = "supervision_district_aggregated_metrics_materialized";
  } else if (system === "INCARCERATION") {
    xAxisColumn = "facility_name";
    tableName = "incarceration_facility_aggregated_metrics_materialized";
  } else {
    throw new Error(
      "Invalid system provided. Please check all Google Form Inputs."
    );
  }
  yAxisColumn = `task_completions_${completionEventType.toLowerCase()}`;

  const queryString = `
    SELECT
      ${xAxisColumn},
      ${yAxisColumn},
    FROM
    \`aggregated_metrics.${tableName}\`
    WHERE state_code = '${stateCode}'
    AND period = '${timePeriod}'
    AND end_date = '${endDateString}';
  `;

  return {
    supervisionDistrictData: runQuery(queryString),
    xAxisColumn: xAxisColumn,
    yAxisColumn: yAxisColumn,
  };
}

/**
 * Construct supervision district column chart
 * Populates a new supervision column chart.
 * @param {string} workflow The name of the workflow
 * @param {string} xAxisColumn The name of the x-axis
 * @param {string} yAxisColumn The name of the y-axis
 * @param {array} supervisionDistrictData An array of arrays containing data for each district/facility
 * @returns {Chart} The built/populated column chart
 */
function constructSupervisionDistrictColumnChart(
  workflow,
  xAxisColumn,
  yAxisColumn,
  supervisionDistrictData
) {
  const xAxisClean = cleanString(xAxisColumn);
  const yAxisClean = cleanString(yAxisColumn);

  const title = `${workflow} Impact`;

  const chartData = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, xAxisColumn)
    .addColumn(Charts.ColumnType.NUMBER, yAxisColumn);

  supervisionColumnChart = createColumnChart(
    supervisionDistrictData,
    chartData,
    title,
    xAxisClean,
    yAxisClean
  );

  return supervisionColumnChart;
}
