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
 * Construct supervision district column chart
 * Given parameters provided by the user, constructs a query string and calls runQuery
 * to query the BiqQuery database. After fetching the data from the database, populates
 * a new supervision column chart.
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} timePeriod The time period passed in from the Google Form (ex: 'MONTH', 'QUARTER', or 'YEAR')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @returns {Chart} The built/populated column chart
 */
function constructSupervisionDistrictColumnChart(
  stateCode,
  timePeriod,
  endDateString
) {
  // TODO(#5805): parameterize this function for any workflow
  const queryString = `
    SELECT
      district_name,
      task_completions_early_discharge,
    FROM
    \`aggregated_metrics.supervision_district_aggregated_metrics_materialized\`
    WHERE state_code = '${stateCode}'
    AND period = '${timePeriod}'
    AND end_date = '${endDateString}';
  `;

  supervisionDistrictData = runQuery(queryString);

  const title = `${stateCode} Early Discharges by District - ${timePeriod.toLowerCase()} ending ${endDateString}`;

  const chartData = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, "district_name")
    .addColumn(Charts.ColumnType.NUMBER, "task_completions_early_discharge");

  supervisionColumnChart = createColumnChart(
    supervisionDistrictData,
    chartData,
    title,
    "District",
    "Early Discharges"
  );

  return supervisionColumnChart;
}
