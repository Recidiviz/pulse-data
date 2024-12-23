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
 * Get MAU By Week Data
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BigQuery database.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @param {string} startDatePlusWeekString A string representing the start_date of the report plus 1 week (ex: '2023-02-08')
 * @param {string} endDatePlusWeekString A string representing the end_date of the report plus 1 week (ex: '2023-03-08')
 * @returns {object} mauByWeekData An array or arrays containing data for each week. Also returns the monthlyActiveUsers string
 */
function getMauByWeekData(
  stateCode,
  completionEventType,
  startDatePlusWeekString,
  endDatePlusWeekString
) {
  const distinctActiveUsers = `distinct_active_users_${completionEventType.toLowerCase()}`;
  const monthlyActiveUsers = "monthly_active_users";

  const mauTable =
    "justice_involved_state_month_rolling_weekly_aggregated_metrics_materialized";

  const queryString = `
    SELECT
      CONCAT(FORMAT_DATE('%m/%d/%y', start_date), " - ", FORMAT_DATE('%m/%d/%y', DATE_SUB(end_date, INTERVAL 1 DAY))) AS formatted_date,
      ${distinctActiveUsers}
    FROM \`impact_reports.${mauTable}\`
    WHERE state_code = '${stateCode}'
    AND end_date >= '${startDatePlusWeekString}'
    AND end_date < '${endDatePlusWeekString}'
    AND ${distinctActiveUsers} IS NOT NULL
    ORDER BY end_date ASC
  `;
  const mauByWeekData = runQuery(queryString);

  return {
    mauByWeekData,
    monthlyActiveUsers,
  };
}

/**
 * Get WAU By Week Data
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BigQuery database.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} startDateString The start date of the report (ex: '2023-02-01')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @returns {object} wauByWeekData An array or arrays containing data for each week. Also returns the weeklyActiveUsers string
 */
function getWauByWeekData(
  stateCode,
  startDateString,
  endDateString,
  completionEventType
) {
  const distinctActiveUsers = `distinct_active_users_${completionEventType.toLowerCase()}`;
  const weeklyActiveUsers = "weekly_active_users";

  const wauTable =
    "justice_involved_state_week_rolling_weekly_aggregated_metrics_materialized";

  const queryString = `
    SELECT
      CONCAT(FORMAT_DATE('%m/%d/%y', start_date), " - ", FORMAT_DATE('%m/%d/%y', DATE_SUB(end_date, INTERVAL 1 DAY))) AS formatted_date,
      ${distinctActiveUsers}
    FROM \`impact_reports.${wauTable}\`
    WHERE state_code = '${stateCode}'
    AND start_date >= '${startDateString}'
    AND start_date < '${endDateString}'
    AND ${distinctActiveUsers} IS NOT NULL
    ORDER BY end_date ASC
  `;
  const wauByWeekData = runQuery(queryString);

  return {
    wauByWeekData,
    weeklyActiveUsers,
  };
}

/**
 * Get MAU WAU By Location
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BigQuery database.
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
 * to query the BigQuery database.
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
 * Construct Statewide MAU and WAU Text
 * Given parameters provided by the user, constructs a query string and call RunQuery
 * to query the BigQuery database. Fetches and returns the total number of statewide distinct monthly active users, statewide distinct monthly registered users, statewide distinct weekly active users, statewide distinct weekly registered users for each system
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @returns {map} an object that contains the number of distinctMonthlyActiveUsersSupervisionTotal, distinctMonthlyRegisteredUsersSupervisionTotal, distinctMonthlyActiveUsersFacilitiesTotal, distinctMonthlyRegisteredUsersFacilitiesTotal, distinctWeeklyActiveUsersSupervisionTotal, distinctWeeklyRegisteredUsersSupervisionTotal, distinctWeeklyActiveUsersFacilitiesTotal, and distinctWeeklyRegisteredUsersFacilitiesTotal
 **/
function constructStatewideMauAndWauText(stateCode, endDateString) {
  const distinctActiveUsersSupervisionTotal = `distinct_active_users_supervision`;
  const distinctRegisteredUsersSupervisionTotal = `distinct_registered_users_supervision`;
  const distinctActiveUsersFacilitiesTotal = `distinct_active_users_incarceration`;
  const distinctRegisteredUsersFacilitiesTotal = `distinct_registered_users_incarceration`;
  const mauTable = `justice_involved_state_month_aggregated_metrics_materialized`;
  const wauTable = `justice_involved_state_week_aggregated_metrics_materialized`;

  const queryStringMonthly = `
    SELECT
      ${distinctActiveUsersSupervisionTotal},
      ${distinctRegisteredUsersSupervisionTotal},
      ${distinctActiveUsersFacilitiesTotal},
      ${distinctRegisteredUsersFacilitiesTotal}
    FROM \`impact_reports.${mauTable}\`
    WHERE state_code = '${stateCode}'
    AND end_date = '${endDateString}'`;

  const queryOutputMonthly = runQuery(queryStringMonthly)[0];
  const queryOutputLengthMonthly = queryOutputMonthly.length;
  if (queryOutputLengthMonthly !== 4) {
    throw new Error(
      `Expected 4 columns in query output, but got length: ${queryOutputLengthMonthly}. Query output: ${queryOutputMonthly}.`
    );
  }

  const queryStringWeekly = `
    SELECT
      ${distinctActiveUsersSupervisionTotal},
      ${distinctRegisteredUsersSupervisionTotal},
      ${distinctActiveUsersFacilitiesTotal},
      ${distinctRegisteredUsersFacilitiesTotal}
    FROM \`impact_reports.${wauTable}\`
    WHERE state_code = '${stateCode}'
    AND end_date = '${endDateString}'`;

  const queryOutputWeekly = runQuery(queryStringWeekly)[0];
  const queryOutputLengthWeekly = queryOutputWeekly.length;
  if (queryOutputLengthWeekly !== 4) {
    throw new Error(
      `Expected 4 columns in query output, but got length: ${queryOutputLengthWeekly}. Query output: ${queryOutputWeekly}.`
    );
  }

  const distinctMonthlyActiveUsersSupervisionTotal = parseInt(
    queryOutputMonthly[0]
  );
  const distinctMonthlyRegisteredUsersSupervisionTotal = parseInt(
    queryOutputMonthly[1]
  );
  const distinctMonthlyActiveUsersFacilitiesTotal = parseInt(
    queryOutputMonthly[2]
  );
  const distinctMonthlyRegisteredUsersFacilitiesTotal = parseInt(
    queryOutputMonthly[3]
  );
  Logger.log(
    "Distinct Monthly Active Users Supervision Total: %s",
    distinctMonthlyActiveUsersSupervisionTotal
  );
  Logger.log(
    "Distinct Monthly Registered Users Supervision Total: %s",
    distinctMonthlyRegisteredUsersSupervisionTotal
  );
  Logger.log(
    "Distinct Monthly Active Users Facilities Total: %s",
    distinctMonthlyActiveUsersFacilitiesTotal
  );
  Logger.log(
    "Distinct Monthly Registered Users Facilities Total: %s",
    distinctMonthlyRegisteredUsersFacilitiesTotal
  );

  const distinctWeeklyActiveUsersSupervisionTotal = parseInt(
    queryOutputWeekly[0]
  );
  const distinctWeeklyRegisteredUsersSupervisionTotal = parseInt(
    queryOutputWeekly[1]
  );
  const distinctWeeklyActiveUsersFacilitiesTotal = parseInt(
    queryOutputWeekly[2]
  );
  const distinctWeeklyRegisteredUsersFacilitiesTotal = parseInt(
    queryOutputWeekly[3]
  );
  Logger.log(
    "Distinct Weekly Active Users Supervision Total: %s",
    distinctWeeklyActiveUsersSupervisionTotal
  );
  Logger.log(
    "Distinct Weekly Registered Users Supervision Total: %s",
    distinctWeeklyRegisteredUsersSupervisionTotal
  );
  Logger.log(
    "Distinct Weekly Active Users Facilities Total: %s",
    distinctWeeklyActiveUsersFacilitiesTotal
  );
  Logger.log(
    "Distinct Weekly Registered Users Facilities Total: %s",
    distinctWeeklyRegisteredUsersFacilitiesTotal
  );

  return {
    distinctMonthlyActiveUsersSupervisionTotal,
    distinctMonthlyRegisteredUsersSupervisionTotal,
    distinctMonthlyActiveUsersFacilitiesTotal,
    distinctMonthlyRegisteredUsersFacilitiesTotal,
    distinctWeeklyActiveUsersSupervisionTotal,
    distinctWeeklyRegisteredUsersSupervisionTotal,
    distinctWeeklyActiveUsersFacilitiesTotal,
    distinctWeeklyRegisteredUsersFacilitiesTotal,
  };
}

/**
 * Construct MAU and WAU By Workflow Text
 * Given parameters provided by the user, constructs a query string and call RunQuery
 * to query the BigQuery database. Fetches and returns the total number of distinct monthly active users, distinct monthly registered users, distinct weekly active users, and distinct weekly registered users for the given workflow
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {string} completionEventType The completion event type of the workflow (what we call it in the database)
 * @param {string} system The system of the workflow (ex: 'SUPERVISION' or 'INCARCERATION')
 * @returns {map} an object that contains the number of distinctMonthlyActiveUsersByWorkflow, distinctMonthlyRegisteredUsers, distinctWeeklyActiveUsersByWorkflow, and distinctWeeklyRegisteredUsers
 **/
function constructMauAndWauByWorkflowText(
  stateCode,
  endDateString,
  completionEventType,
  system
) {
  const distinctActiveUsersByWorkflow = `distinct_active_users_${completionEventType.toLowerCase()}`;
  const distinctRegisteredUsers = `distinct_registered_users_${system.toLowerCase()}`;
  const mauTable = `justice_involved_state_month_aggregated_metrics_materialized`;
  const wauTable = `justice_involved_state_week_aggregated_metrics_materialized`;

  const queryStringMonthly = `
    SELECT
      ${distinctActiveUsersByWorkflow},
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
      ${distinctActiveUsersByWorkflow},
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

  const distinctMonthlyActiveUsersByWorkflow = parseInt(queryOutputMonthly[0]);
  const distinctMonthlyRegisteredUsers = parseInt(queryOutputMonthly[1]);
  Logger.log(
    "Distinct Monthly Active Users by Workflow: %s",
    distinctMonthlyActiveUsersByWorkflow
  );
  Logger.log(
    "Distinct Monthly Registered Users: %s",
    distinctMonthlyRegisteredUsers
  );

  const distinctWeeklyActiveUsersByWorkflow = parseInt(queryOutputWeekly[0]);
  const distinctWeeklyRegisteredUsers = parseInt(queryOutputWeekly[1]);
  Logger.log(
    "Distinct Weekly Active Users by Workflow: %s",
    distinctWeeklyActiveUsersByWorkflow
  );
  Logger.log(
    "Distinct Weekly Registered Users: %s",
    distinctWeeklyRegisteredUsers
  );

  return {
    distinctMonthlyActiveUsersByWorkflow,
    distinctMonthlyRegisteredUsers,
    distinctWeeklyActiveUsersByWorkflow,
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
 * to query the BigQuery database. After fetching the total number of individuals surfaced, eligible, reviewed, and ineligible, adds them and returns the integer as a formatted
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
 * to query the BigQuery database. After fetching the total number of supervision and
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
 * to query the BigQuery database.
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
    yAxisClean,
    undefined, // set colors to default
    undefined, // default stack setting
    undefined, // default width
    undefined, // default height
    undefined, // default legend position
    false // do not filter out zero values
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
    Charts.Position.BOTTOM, // this chart has a legend
    false // for this chart, we do not want to filter out zero values
  );

  return mauWauByLocationColumnChart;
}

/**
 * Construct active users by week column chart
 * Populates a new wau or mau by week column chart.
 * @param {string} activeUsersLabel The name of the column to be charts (ex: weekly_active_users or monthly_active_users)
 * @param {array} wauAndMauByWeekData An array of arrays containing data for each week
 * @param {string} titleString A string that will become the title of the chart
 * @param {array} customColor An array containing a string with the custom color of the column chart
 * @returns {Chart} The built/populated column chart
 */
function constructActiveUsersByWeekColumnChart(
  activeUsersLabel,
  wauAndMauByWeekData,
  titleString,
  customColor = ["#3697FA"],
) {
  const xAxisClean = "Start Date - End Date";
  const activeUsersLabelClean = cleanString(activeUsersLabel);

  const chartData = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, xAxisClean)
    .addColumn(Charts.ColumnType.NUMBER, activeUsersLabelClean);

  wauAndMauByWeekColumnChart = createColumnChart(
    wauAndMauByWeekData,
    chartData,
    titleString,
    xAxisClean,
    "# of Users",
    customColor, // MAU chart is blue, WAU chart is red
    false, // we do not want to stack columns
    1278, // chart width
    910, // chart height
    undefined, // this chart does not have a legend
    false // for this chart, we do not want to filter out zero values
  );

  return wauAndMauByWeekColumnChart;
}
