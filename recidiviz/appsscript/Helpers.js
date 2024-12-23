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

const REGIONS_AND_FACILITIES_SUBSTRINGS_TO_FILTER_OUT = [
  "NOT_APPLICABLE",
  "EXTERNAL_UNKNOWN",
  "UNKNOWN",
  "NULL",
  "UNKNOWN LOCATION",
  "COUNTY JAIL",
  "FEDERAL",
  "ARIZONA",
  "JAIL",
];

/**
 * Run Query
 * Given a query string, runs the query against the BigQuery database (synchronously)
 * and returns the query results as an array of arrays.
 * @param {string} queryString The SQL query string to run in BigQuery
 * @returns {array} An array containing arrays (that represent rows) of data
 */
function runQuery(queryString) {
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
    queryResults = BigQuery.Jobs.getQueryResults(
      projectId,
      queryResults.jobReference.jobId
    );
  }

  // Get all the rows of results.
  let rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(
      projectId,
      queryResults.jobReference.jobId,
      {
        pageToken: queryResults.pageToken,
      }
    );
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
 * @returns {Chart} The built/populated column chart or null if there is no data to display (all 0 values)
 */
function createColumnChart(
  data,
  chartData,
  title,
  xAxis,
  yAxis,
  setColors = ["#3697FA"], // default all charts to blue
  stacked = false,
  width = 1278,
  height = 910,
  legend = Charts.Position.NONE,
  filterOutZero = true
) {
  const enCollator = new Intl.Collator("en", { numeric: true });
  let buildChart = false;
  data.sort(enCollator.compare).forEach((newRow) => {
    if (
      !newRow[0] ||
      REGIONS_AND_FACILITIES_SUBSTRINGS_TO_FILTER_OUT.some((excludeTerm) =>
        newRow[0].toUpperCase().includes(excludeTerm)
      ) ||
      (filterOutZero && parseInt(newRow[1]) === 0)
    ) {
      return;
    }
    newRow[0] = toTitleCase(newRow[0]);
    chartData.addRow(newRow);
    buildChart = true;
  });

  if (buildChart === false) {
    return null;
  }

  chartData.build();

  let chart = Charts.newColumnChart()
    .setDataTable(chartData)
    .setXAxisTitle(xAxis)
    .setLegendPosition(legend)
    .setDimensions(width, height)
    .setOption("fontSize", 40)
    .setTitleTextStyle(Charts.newTextStyle().setFontSize(28).build())
    .setXAxisTitleTextStyle(Charts.newTextStyle().setFontSize(20))
    .setYAxisTitleTextStyle(Charts.newTextStyle().setFontSize(20))
    .setXAxisTextStyle(Charts.newTextStyle().setFontSize(12))
    .setYAxisTextStyle(Charts.newTextStyle().setFontSize(20))
    .setLegendTextStyle(Charts.newTextStyle().setFontSize(20))
    .setOption("chartArea.top", 50)
    .setOption("chartArea.width", "65%")
    .setColors(setColors);

  if (title) {
    chart = chart.setTitle(title);
  }
  if (yAxis) {
    chart = chart.setYAxisTitle(yAxis);
  }

  if (stacked) {
    chart = chart.setStacked();
  }

  chart = chart.build();

  return chart;
}

/**
 * Clean date
 * @param {string} dbDate The string representation of the date used in the database (ex: 2023-03-01)
 * @returns {string} cleanDate The cleaned version of the string/date (ex: 03/01/2023)
 */
function cleanDate(dbDate) {
  const splitDate = dbDate.split("-");
  const cleanDate = `${splitDate[1]}/${splitDate[2]}/${splitDate[0]}`;

  return cleanDate;
}

/**
 * Clean string
 * @param {string} dbString The string/label from the database
 * @returns {string} cleanString The cleaned version of the string/label (camelCase)
 */
function cleanString(dbString) {
  let cleanString = dbString.toLowerCase();

  cleanString = cleanString.replace("task_completions_", "");

  cleanString = cleanString
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");

  return cleanString;
}

/**
 * Get index of element to replace
 * Find the index of the element we want to copy and replace
 * @param {Body} body The template document body
 * @param {Enum} elementType The type of element in which we are replacing text
 * @param {string} textToMatch The string that we are replacing
 * @returns {Number} childIdx The index of the child element in which we will copy and replace
 */
function getIndexOfElementToReplace(body, elementType, textToMatch) {
  const totalChildren = body.getNumChildren();
  var childIdx = null;

  for (let idx = 0; idx !== totalChildren; idx++) {
    let child = body.getChild(idx);

    if (child.getType() === elementType) {
      if (elementType === DocumentApp.ElementType.TABLE) {
        // At this point, the template table is 5 rows by 4 columns
        // We want to get the cell in the fourth row, second column
        // Note that cells start at index 0
        child = child.getCell(3, 1);
      }

      if (child.getText() === textToMatch) {
        // We found the start of where we want to copy
        childIdx = idx;
        break;
      }
    }
  }

  return childIdx;
}

/**
 * Calculate active users percent
 * Helper function to calculate the percent of active users based on the number of active users and the number of registered users for a given workflow
 * @param {Number} activeUsers The number of active users for the given workflow
 * @param {Number} registeredUsers The number of registered users for the given workflow
 * @returns {string} The percent of active users rounded to two decimal places
 */
function calculateActiveUsersPercent(activeUsers, registeredUsers) {
  if (registeredUsers === 0) {
    return 0;
  }
  return ((activeUsers / registeredUsers) * 100).toFixed(1);
}

/**
 * Converts a given string, str, to title case.
 * src: https://stackoverflow.com/questions/196972/convert-string-to-title-case-with-javascript
 */
function toTitleCase(str) {
  return str.replace(
    /\w\S*/g,
    (text) => text.charAt(0).toUpperCase() + text.substring(1).toLowerCase()
  );
}

/**
 * Get bounds date strings
 * Given a startDateString or endDateString (provided by the user via google form), construct a string representing the startDateString or endDateString plus 1 week. These strings will be used in the getMauByWeekData() function to query for the MAU for the appropriate time ranges.
 * @param {string} cleanDateString A string representing the start date of the report or end date of the report (ex: 2024-10-01 or 2024-11-01)
 * @returns {string} datePlusWeekString A string representing the start date plus 1 week or the end date plus 1 week (ex: 2024-10-08 and 2024-11-08)
 */
function getBoundsDateString(
  cleanDateString
) {
  const datestringSplit = cleanDateString.split("-");

  // since the local timezone is set to Eastern Daylight Time, we need to add 5 hours to be UTC
  let utcDate = new Date(
    Date.UTC(
      datestringSplit[0], // year
      datestringSplit[1] - 1, // month (indexed 0-11)
      datestringSplit[2], // day
      5 // hour
    )
  );
  utcDate.setDate(utcDate.getDate() + 7);
  const datePlusWeekString = utcDate.toLocaleDateString("en-CA");

  return datePlusWeekString;
}
