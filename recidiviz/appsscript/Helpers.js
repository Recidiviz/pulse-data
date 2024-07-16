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
/* Helper file used by CreateReport.gs. */

/**
 * Run Query
 * Given a query string, runs the query against the BigQuery database (synchronously)
 * and returns the query results as an array of arrays.
 * @param {string} queryString The SQL query string to run in BigQuery
 * @returns {array} An array containing arrays (that represent rows) of data
 */
function RunQuery(queryString) {
  const request = {
    query: queryString,
    useLegacySql: false,
  };
  let queryResults = BigQuery.Jobs.query(request, projectId);

  // Check on status of the Query Job.
  let sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Get all the rows of results.
  let rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken,
    });
    rows = rows.concat(queryResults.rows);
  }

  if (!rows) {
    console.log("No rows returned.");
    return;
  }

  // Append the results
  const data = new Array(rows.length);
  for (let i = 0; i < rows.length; i++) {
    const cols = rows[i].f;
    data[i] = new Array(cols.length);
    for (let j = 0; j < cols.length; j++) {
      data[i][j] = cols[j].v;
    }
  }

  return data;
}

/**
 * Create column chart
 * Builds and populates a new column chart.
 * @param {array} data An array containing arrays (that represent rows) of data
 * @param {DataTableBuilder} chartData An empty data table that will be populated with data
 * @param {string} title The title of the chart
 * @param {string} xAxis The x-axis lable of the chart
 * @param {string} yAxis The y-axis lable of the chart
 * @returns {Chart} The built/populated column chart
 */
function CreateColumnChart(data, chartData, title, xAxis, yAxis) {
  data.forEach((newRow) => {
    chartData.addRow(newRow);
  });
  chartData.build();

  var chart = Charts.newColumnChart()
    .setDataTable(chartData)
    .setTitle(title)
    .setXAxisTitle(xAxis)
    .setYAxisTitle(yAxis)
    .build();

  return chart;
}
