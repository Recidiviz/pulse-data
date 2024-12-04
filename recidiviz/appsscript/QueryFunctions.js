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
 * Get WAU and MAU By Week Data
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BiqQuery database.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} startDateString The start date of the report (ex: '2023-02-01')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @returns {object} wauByWeekData An array or arrays containing data for each week. Also returns the weeklyActiveUsers string
 */
function getWauAndMauByWeekData(
  stateCode,
  startDateString,
  endDateString,
  completionEventType
) {
  const distinctActiveUsers = `distinct_active_users_${completionEventType.toLowerCase()}`;
  const weeklyActiveUsers = "weekly_active_users";
  const monthlyActiveUsers = "monthly_active_users";

  const wauTable = "justice_involved_state_week_rolling_weekly_aggregated_metrics_materialized";
  const mauTable = "justice_involved_state_month_rolling_weekly_aggregated_metrics_materialized";

  const queryString = `
    SELECT
      FORMAT_DATE('%m/%d/%y', end_date) AS formatted_date,
      w.${distinctActiveUsers} AS ${weeklyActiveUsers},
      m.${distinctActiveUsers} AS ${monthlyActiveUsers}
    FROM \`impact_reports.${mauTable}\` m 
    INNER JOIN \`impact_reports.${wauTable}\` w
    USING (state_code, end_date)
    WHERE state_code = '${stateCode}'
    AND w.start_date >= '${startDateString}'
    AND w.start_date < '${endDateString}'
    AND NOT (w.${distinctActiveUsers} IS NULL OR m.${distinctActiveUsers} IS NULL)
    ORDER BY w.start_date ASC
  `;

  const wauAndMauByWeekData = runQuery(queryString);

  return {
    wauAndMauByWeekData,
    weeklyActiveUsers,
    monthlyActiveUsers,
  };
}

/**
 * Get MAU WAU By Location
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BiqQuery database.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @param {string} system The system of the workflow (ex: 'SUPERVISION' or 'INCARCERATION')
 * @returns {object} mauWauByLocationData An array or arrays containing data for each district/facility. Also returns the mauWauXAxisColumn, mau, and wau strings
 */
function getMauWauByLocation(
  stateCode,
  endDateString,
  completionEventType,
  system
) {
  const distinctActiveUsers = `distinct_active_users_${completionEventType.toLowerCase()}`;
  const mau = "monthly_active_users";
  const wau = "weekly_active_users";

  let mauWauXAxisColumn = "";
  let location = "";

  if (system === "SUPERVISION") {
    mauWauXAxisColumn = "district_name";
    location = "district";
  } else if (system === "INCARCERATION") {
    mauWauXAxisColumn = "facility_name";
    location = "facility";
  } else {
    throw new Error(
      "Invalid system provided. Please check all Google Form Inputs."
    );
  }

  const mauTable = `justice_involved_${location}_month_aggregated_metrics_materialized`;
  const wauTable = `justice_involved_${location}_week_aggregated_metrics_materialized`;

  const queryString = `
    SELECT
      ${location},
      ${mauTable}.${distinctActiveUsers} AS ${mau},
      ${wauTable}.${distinctActiveUsers} AS ${wau}
    FROM \`impact_reports.${mauTable}\` ${mauTable}
    INNER JOIN \`impact_reports.${wauTable}\` ${wauTable}
    USING (${location}, state_code, end_date)
    WHERE ${mauTable}.state_code = '${stateCode}'
    AND ${mauTable}.end_date = '${endDateString}'
    AND NOT (${mauTable}.${distinctActiveUsers} IS NULL OR ${wauTable}.${distinctActiveUsers} IS NULL)`;

  const mauWauByLocationData = runQuery(queryString);

  return {
    mauWauByLocationData,
    mauWauXAxisColumn,
    mau,
    wau,
  };
}

/**
 * Get usage and impact district data
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BiqQuery database.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @param {string} system The system of the workflow (ex: 'SUPERVISION' or 'INCARCERATION')
 * @returns {object} usageAndImpactDistrictData An array or arrays containing data for each district/facility. Also returns the usageAndImpactXAxisColumn, eligibleAndViewedColumn, markedIneligibleColumn, and eligibleAndNotViewedColumn
 */
function getUsageAndImpactDistrictData(
  stateCode,
  endDateString,
  completionEventType,
  system
) {
  let usageAndImpactXAxisColumn = "";
  let tableName = "";

  if (system === "SUPERVISION") {
    usageAndImpactXAxisColumn = "district_name";
    tableName = "justice_involved_district_day_aggregated_metrics_materialized";
  } else if (system === "INCARCERATION") {
    usageAndImpactXAxisColumn = "facility_name";
    tableName = "justice_involved_facility_day_aggregated_metrics_materialized";
  } else {
    throw new Error(
      "Invalid system provided. Please check all Google Form Inputs."
    );
  }

  const eligibleAndViewedColumn = `avg_daily_population_task_eligible_and_viewed_${completionEventType.toLowerCase()}`;
  const markedIneligibleColumn = `avg_daily_population_task_marked_ineligible_${completionEventType.toLowerCase()}`;
  const eligibleAndNotViewedColumn = `avg_daily_population_task_eligible_and_not_viewed_${completionEventType.toLowerCase()}`;

  const queryString = `
    SELECT
      ${usageAndImpactXAxisColumn},
      ${eligibleAndViewedColumn},
      ${markedIneligibleColumn},
      ${eligibleAndNotViewedColumn}
    FROM
    \`impact_reports.${tableName}\`
    WHERE state_code = '${stateCode}'
    AND end_date = '${endDateString}';
  `;

  const usageAndImpactDistrictData = runQuery(queryString);

  return {
    usageAndImpactDistrictData,
    usageAndImpactXAxisColumn,
    eligibleAndViewedColumn,
    markedIneligibleColumn,
    eligibleAndNotViewedColumn,
  };
}

/**
 * Construct MAU and WAU Text
 * Given parameters provided by the user, constructs a query string and call RunQuery
 * to query the BiqQuery database. Fetches and returns the total number of distinct monthly active users, distinct monthly registered users, distinct weekly active users, and distinct weekly registered users for the given workflow
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @param {string} system The system of the workflow (ex: 'SUPERVISION' or 'INCARCERATION')
 * @returns {map} an object that contains the number of distinctMonthlyActiveUsers, distinctMonthlyRegisteredUsers, distinctWeeklyActiveUsers, and distinctWeeklyRegisteredUsers
 **/
function constructMauAndWauText(
  stateCode,
  endDateString,
  completionEventType,
  system
) {
  const distinctActiveUsers = `distinct_active_users_${completionEventType.toLowerCase()}`;
  const distinctRegisteredUsers = `distinct_registered_users_${system.toLowerCase()}`;
  const mauTable = `justice_involved_state_month_aggregated_metrics_materialized`;
  const wauTable = `justice_involved_state_week_aggregated_metrics_materialized`;

  const queryStringMonthly = `
    SELECT
      ${distinctActiveUsers},
      ${distinctRegisteredUsers}
    FROM \`impact_reports.${mauTable}\`
    WHERE state_code = '${stateCode}'
    AND end_date = '${endDateString}'`;

  const queryOutputMonthly = runQuery(queryStringMonthly)[0];
  const queryOutputLengthMonthly = queryOutputMonthly.length;
  if (queryOutputLengthMonthly !== 2) {
    throw new Error(
      `Expected 2 columns in query output, but got length: ${queryOutputLengthMonthly}. Query output: ${queryOutputMonthly}.`
    );
  }

  const queryStringWeekly = `
    SELECT
      ${distinctActiveUsers},
      ${distinctRegisteredUsers}
    FROM \`impact_reports.${wauTable}\`
    WHERE state_code = '${stateCode}'
    AND end_date = '${endDateString}'`;

  const queryOutputWeekly = runQuery(queryStringWeekly)[0];
  const queryOutputLengthWeekly = queryOutputWeekly.length;
  if (queryOutputLengthWeekly !== 2) {
    throw new Error(
      `Expected 2 columns in query output, but got length: ${queryOutputLengthWeekly}. Query output: ${queryOutputWeekly}.`
    );
  }

  const distinctMonthlyActiveUsers = parseInt(queryOutputMonthly[0]);
  const distinctMonthlyRegisteredUsers = parseInt(queryOutputMonthly[1]);
  Logger.log("Distinct Monthly Active Users: %s", distinctMonthlyActiveUsers);
  Logger.log(
    "Distinct Monthly Registered Users: %s",
    distinctMonthlyRegisteredUsers
  );

  const distinctWeeklyActiveUsers = parseInt(queryOutputWeekly[0]);
  const distinctWeeklyRegisteredUsers = parseInt(queryOutputWeekly[1]);
  Logger.log("Distinct Weekly Active Users: %s", distinctWeeklyActiveUsers);
  Logger.log(
    "Distinct Weekly Registered Users: %s",
    distinctWeeklyRegisteredUsers
  );

  return {
    distinctMonthlyActiveUsers,
    distinctMonthlyRegisteredUsers,
    distinctWeeklyActiveUsers,
    distinctWeeklyRegisteredUsers,
  };
}

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

  const startDateString = queryOutput[0];
  const splitStartDate = startDateString.split("-");
  const startDate = `${splitStartDate[1]}/${splitStartDate[2]}/${splitStartDate[0]}`;
  Logger.log("startDate: %s", startDate);

  return { startDate, startDateString };
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
    almostEligible,
    eligible,
    markedIneligible,
    eligibleAndViewed,
    eligibleAndNotViewed,
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
 * Get max locations
 * Given an array of arrays that contain a datapoint/value for each location, return an array containing the location names associated with the max value as well as the max value.
 * @param {array} locationData An array of arrays containing locations and values (ex: [["Region 1", 90], ["Region 5", 25]])
 * @returns {map} an array of the location names with max value and the max value itself
 **/
function getMaxLocations(locationData) {
  var maxLocations = [];
  var maxValue = 0;
  locationData.forEach((arr) => {
    if (parseFloat(arr[1]) > parseFloat(maxValue)) {
      // We have a new max value and location
      maxValue = arr[1];
      maxLocations = [arr[0]];
    } else if (parseFloat(arr[1]) === parseFloat(maxValue) && maxValue !== 0) {
      // We have a tied max value and location
      maxLocations.push(arr[0]);
    }
  });

  Logger.log("{ maxLocations, maxValue }: %s", { maxLocations, maxValue });
  return { maxLocations, maxValue };
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

/**
 * Construct usage and impact district column chart
 * Populates a new usage and impact column chart.
 * @param {string} xAxisColumn The name of the x-axis
 * @param {string} eligibleAndViewedColumn The name of the eligibleAndViewedColumn
 * @param {string} markedIneligibleColumn The name of the markedIneligibleColumn
 * @param {string} eligibleAndNotViewedColumn The name of the eligibleAndNotViewedColumn
 * @param {array} usageAndImpactDistrictData An array of arrays containing data for each district/facility
 * @returns {Chart} The built/populated column chart
 */
function constructUsageAndImpactDistrictColumnChart(
  xAxisColumn,
  eligibleAndViewedColumn,
  markedIneligibleColumn,
  eligibleAndNotViewedColumn,
  usageAndImpactDistrictData
) {
  const xAxisClean = cleanString(xAxisColumn);
  const eligibleAndViewedAxisClean = cleanString(eligibleAndViewedColumn);
  const markedIneligibleAxisClean = cleanString(markedIneligibleColumn);
  const eligibleAndNotViewedAxisClean = cleanString(eligibleAndNotViewedColumn);

  const chartData = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, xAxisColumn)
    .addColumn(Charts.ColumnType.NUMBER, eligibleAndViewedAxisClean)
    .addColumn(Charts.ColumnType.NUMBER, markedIneligibleAxisClean)
    .addColumn(Charts.ColumnType.NUMBER, eligibleAndNotViewedAxisClean);

  // Since this chart does not have a title or a y-axis label, we pass those arguments in as null
  // We pass setColors in as true since this chart has some custom colors
  // We pass stacked in as true since this chart is a stacked column chart
  usageAndImpactDistrictColumnChart = createColumnChart(
    usageAndImpactDistrictData,
    chartData,
    null, // title
    xAxisClean,
    null, // y-axis
    ["#3697FA", "#BABABA", "#CA2E17"], // this chart will have blue, gray, and red columns
    true, // stacked
    undefined,
    undefined,
    undefined,
    false // for this chart, we do not want to filter out zero values
  );

  return usageAndImpactDistrictColumnChart;
}

/**
 * Construct Mau Wau by location column chart
 * Populates a new mau/wau by location column chart.
 * @param {string} mauWauXAxisColumn The name of the x-axis
 * @param {array} mauWauByLocationData An array of arrays containing data for each district/facility
 * @param {string} mau The name of the mau column
 * @param {string} wau The name of the wau column
 * @returns {Chart} The built/populated column chart
 */
function constructMauWauByLocationColumnChart(
  mauWauXAxisColumn,
  mauWauByLocationData,
  mau,
  wau
) {
  const xAxisClean = cleanString(mauWauXAxisColumn);
  const mauClean = cleanString(mau);
  const wauClean = cleanString(wau);

  const chartData = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, xAxisClean)
    .addColumn(Charts.ColumnType.NUMBER, mauClean)
    .addColumn(Charts.ColumnType.NUMBER, wauClean);

  mauWauByLocationColumnChart = createColumnChart(
    mauWauByLocationData,
    chartData,
    "Region Variation in Usage",
    xAxisClean,
    "# of Users",
    ["#48742C", "#F2BF40"], // this chart will have green and yellow columns
    false, // we do not want to stack columns
    1022, // this chart has a custom width
    618, // this chart has a custom height
    Charts.Position.BOTTOM // this chart has a legend
  );

  return mauWauByLocationColumnChart;
}

/**
 * Construct WAU and MAU by week column chart
 * Populates a new wau by week column chart.
 * @param {string} weeklyActiveUsers The name of the weeklyActiveUsers column
 * @param {string} monthlyActiveUsers The name of the monthlyActiveUsers column
 * @param {array} wauAndMauByWeekData An array of arrays containing data for each week
 * @returns {Chart} The built/populated column chart
 */
function constructWauAndMauByWeekColumnChart(
  monthlyActiveUsers,
  weeklyActiveUsers,
  wauAndMauByWeekData
) {
  const xAxisClean = "End Date";
  const wauClean = cleanString(weeklyActiveUsers);
  const mauClean = cleanString(monthlyActiveUsers);

  const chartData = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, xAxisClean)
    .addColumn(Charts.ColumnType.NUMBER, wauClean)
    .addColumn(Charts.ColumnType.NUMBER, mauClean);

  wauAndMauByWeekColumnChart = createColumnChart(
    wauAndMauByWeekData,
    chartData,
    "Weekly and Monthly Active Users",
    xAxisClean,
    "# of Users",
    ["#CA2E17", "#3697FA"], // this chart will have red and blue columns
    false, // we do not want to stack columns
    1278, // chart width
    910, // chart height
    Charts.Position.BOTTOM, // this chart has a legend
    false // for this chart, we do not want to filter out zero values
  );

  return wauAndMauByWeekColumnChart;
}
