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
/* Apps Script for generating Leadership Impact Usage Reports. */

const projectId = "recidiviz-staging";

/**
 * Main
 * This is the entry point of the script. The main function is triggered by the
 * submission of the connected Google Form. It takes in response items provided by the
 * user. It then calls ConstructSupervisionDistrictColumnChart to construct a column
 * chart. It then calls copyTemplateDoc to copy the template document and populate it
 * with the new column chart.
 * @param {object} e The object passed from the connected Google Form on form submit
 */
function main(e) {
  const response = e.response;

  const itemResponses = response.getItemResponses();

  const stateCode = itemResponses[0].getResponse();
  const timePeriod = itemResponses[1].getResponse();
  const endDateString = itemResponses[2].getResponse();

  Logger.log("stateCode: %s", stateCode);
  Logger.log("timePeriod: %s", timePeriod);
  Logger.log("endDateString: %s", endDateString);

  supervisionColumnChart = constructSupervisionDistrictColumnChart(
    stateCode,
    timePeriod,
    endDateString
  );

  copyTemplateDoc(supervisionColumnChart, stateCode);
}

/**
 * Copy template doc
 * Copies the template document and stores it in the shared Google Drive folder.
 * Replaces all instances of {{today_date}} with the current date. Replaces the
 * placeholder column chart with the newly populated column chart.
 * @param {Chart} supervisionColumnChart The built/populated column chart
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 */
function copyTemplateDoc(supervisionColumnChart, stateCode) {
  const template = DriveApp.getFileById(
    "1nsc_o2fTlldTQavxJveucWgDKkic_clKZjn0GuyF2N8"
  );
  const destinationFolder = DriveApp.getFolderById(
    "1UzFS4GsbgIqoLBUv0Z314-51pxQznHaJ"
  );

  const today = Utilities.formatDate(new Date(), "GMT-5", "MM/dd/yyyy");

  const copy = template.makeCopy(
    `${stateCode} ${today} Impact Report`,
    destinationFolder
  );
  const doc = DocumentApp.openById(copy.getId());
  const body = doc.getBody();

  // Removes the warning note from the top of the document
  const rangeElementToRemove = body.findText("{{NOTE: .*}}");
  const startOffset = rangeElementToRemove.getStartOffset();
  // Adding 1 to the endOffset to include the new line character
  const endOffset = rangeElementToRemove.getEndOffsetInclusive() + 1
  body.editAsText().deleteText(startOffset, endOffset);

  body.replaceText("{{today_date}}", today);

  const images = body.getImages();
  var imageToReplace = null;
  images.forEach((img) => {
    const altTitle = img.getAltTitle();
    if (altTitle === "Impact Column Chart") {
      imageToReplace = img;
    }
  });
  const imageParent = imageToReplace.getParent();

  imageToReplace.removeFromParent();
  imageParent.appendInlineImage(supervisionColumnChart);
}
