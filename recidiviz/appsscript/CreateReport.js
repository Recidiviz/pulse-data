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
 * chart. It then calls copyAndPopulateTemplateDoc to copy the template document and populate it
 * with the new column chart.
 * @param {object} e The object passed from the connected Google Form on form submit
 */
function main(e) {
  const response = e.response;

  const itemResponses = response.getItemResponses();

  const stateCode = itemResponses[0].getResponse();
  const timePeriod = itemResponses[1].getResponse();
  const endDateString = itemResponses[2].getResponse();
  const workflowsToInclude = itemResponses[3].getResponse();

  Logger.log("stateCode: %s", stateCode);
  Logger.log("timePeriod: %s", timePeriod);
  Logger.log("endDateString: %s", endDateString);
  Logger.log("workflowsToInclude: %s", workflowsToInclude);

  const startDate = getStartDate(stateCode, timePeriod, endDateString);

  const workflowToDistrictOrFacilitiesColumnChart = {};
  const workflowToOpportunityGranted = {};
  const workflowToMaxOpportunityGrantedLocation = {};

  const stateCodeToRows = getSheetValues();
  const stateRows = stateCodeToRows[stateCode];

  workflowsToInclude.forEach((workflow) => {
    var workflowRow = {};
    stateRows.forEach((row) => {
      if (row.workflow === workflow) {
        workflowRow = row;
      }
    });
    Logger.log("workflowRow: %s", workflowRow);
    const completionEventType = workflowRow.completionEventType;
    Logger.log("completionEventType: %s", completionEventType);
    const system = workflowRow.system;
    Logger.log("system: %s", system);

    var opportunityGranted = constructOpportunitiesGrantedText(
      stateCode,
      timePeriod,
      endDateString,
      completionEventType,
      system
    );

    const { supervisionDistrictData, xAxisColumn, yAxisColumn } =
      getSupervisionDistrictData(
        stateCode,
        timePeriod,
        endDateString,
        completionEventType,
        system
      );

    const maxRegion = getMaxRegion(supervisionDistrictData);

    var districtOrFacilitiesColumnChart =
      constructSupervisionDistrictColumnChart(
        workflow,
        xAxisColumn,
        yAxisColumn,
        supervisionDistrictData
      );

    workflowToDistrictOrFacilitiesColumnChart[workflow] =
      districtOrFacilitiesColumnChart;
    workflowToOpportunityGranted[workflow] = opportunityGranted;
    workflowToMaxOpportunityGrantedLocation[workflow] = maxRegion;
  });

  copyAndPopulateTemplateDoc(
    workflowToDistrictOrFacilitiesColumnChart,
    stateCode,
    timePeriod,
    endDateString,
    workflowsToInclude,
    workflowToOpportunityGranted,
    startDate,
    workflowToMaxOpportunityGrantedLocation
  );
}

/**
 * Copy and populate template doc
 * Copies the template document and stores it in the shared Google Drive folder.
 * Replaces all instances of {{today_date}} with the current date. Replaces the
 * placeholder column chart with the newly populated column chart.
 * @param {map} workflowToDistrictOrFacilitiesColumnChart An object that maps the workflow name to its districtOrFacilitiesColumnChart
 * @param {string} stateCode The state code passed in from the Google Form (ex: 'US_MI')
 * @param {string} timePeriod The time period passed in from the Google Form (ex: 'MONTH', 'QUARTER', or 'YEAR')
 * @param {string} endDateString The end date passed from the connected Google Form on form submit (ex: '2023-03-01')
 * @param {array} workflowsToInclude A list of Workflows to be included in the report
 * @param {map} workflowToOpportunityGranted An object that maps the workflow name to the number of opportunities granted for that workflow
 * @param {string} startDate The start date (queried from BigQuery) (ex: '2023-02-01')
 * @param {map} workflowToMaxOpportunityGrantedLocation An object that maps the workflow name to the district or facility with the most number of opportunities granted
 */
function copyAndPopulateTemplateDoc(
  workflowToDistrictOrFacilitiesColumnChart,
  stateCode,
  timePeriod,
  endDateString,
  workflowsToInclude,
  workflowToOpportunityGranted,
  startDate,
  workflowToMaxOpportunityGrantedLocation
) {
  const template = DriveApp.getFileById(
    "1nsc_o2fTlldTQavxJveucWgDKkic_clKZjn0GuyF2N8"
  );
  const destinationFolder = DriveApp.getFolderById(
    "1UzFS4GsbgIqoLBUv0Z314-51pxQznHaJ"
  );

  const today = Utilities.formatDate(new Date(), "GMT-5", "MM/dd/yyyy");

  const copy = template.makeCopy(
    `${stateCode} ${timePeriod.toLowerCase()}ly Impact Report ending ${endDateString}`,
    destinationFolder
  );
  const doc = DocumentApp.openById(copy.getId());
  const body = doc.getBody();

  // Removes the warning note from the top of the document
  const rangeElementToRemove = body.findText("{{NOTE: .*}}");
  const startOffset = rangeElementToRemove.getStartOffset();
  // Adding 1 to the endOffset to include the new line character
  const endOffset = rangeElementToRemove.getEndOffsetInclusive() + 1;
  body.editAsText().deleteText(startOffset, endOffset);

  body.replaceText("{{today_date}}", today);

  const endDateClean = cleanDate(endDateString);
  const timeRange = `${startDate}-${endDateClean}`;
  body.replaceText("{{time_range}}", timeRange);

  copyAndPopulateOpportunityGrants(body, workflowToOpportunityGranted);

  copyAndPopulateWorkflowSection(
    body,
    workflowsToInclude,
    workflowToDistrictOrFacilitiesColumnChart,
    workflowToOpportunityGranted,
    startDate,
    workflowToMaxOpportunityGrantedLocation
  );
}

/**
 * Copy and populate opportunity grants
 * Identifies, copies, and populates the total number of opportunuties granted (over all workflows) as well as the number of opportunities granted for each workflow.
 * @param {Body} body The template document body
 * @param {map} workflowToOpportunityGranted An object that maps the workflow name to the number of opportunities granted for that workflow
 */
function copyAndPopulateOpportunityGrants(body, workflowToOpportunityGranted) {
  // First, populate the sum of all Opportunities Granted (for all Workflows)
  var totalOpportunitiesGranted = 0;
  Object.values(workflowToOpportunityGranted).forEach((opportunityGranted) => {
    totalOpportunitiesGranted += opportunityGranted;
  });
  totalOpportunitiesGranted = totalOpportunitiesGranted.toLocaleString();
  Logger.log("totalOpportunitiesGranted: %s", totalOpportunitiesGranted);
  body.replaceText("{{opp_grants}}", totalOpportunitiesGranted);

  // Next, populate the Opportunities Granted for each individual Workflow
  const childIdx = getIndexOfElementToReplace(
    body,
    DocumentApp.ElementType.TABLE,
    "{{grant_type}}: {{num_grants}} Grants"
  );

  const child = body.getChild(childIdx);
  const cell = child.getCell(2, 1);
  const textToReplace = cell.getChild(0);

  // For each Workflow, copy the placeholder text and populate with the number of Opportunities Granted
  var style = {};
  style[DocumentApp.Attribute.BOLD] = true;

  Object.entries(workflowToOpportunityGranted).forEach(
    ([workflow, opportunityGranted]) => {
      const textCopy = textToReplace.copy();

      textCopy.replaceText(
        "{{grant_type}}: {{num_grants}} Grants",
        `${workflow}: ${opportunityGranted.toLocaleString()} Grants`
      );
      const startBoldIndex =
        textCopy.findText(": ").getEndOffsetInclusive() + 1;
      const endBoldIndex = textCopy.findText("Grants").getEndOffsetInclusive();
      textCopy.editAsText().setAttributes(startBoldIndex, endBoldIndex, style);
      cell.appendParagraph(textCopy);
    }
  );

  // Finally, delete the original element that we copied
  cell.removeChild(textToReplace);
}

/**
 * Copy and populate workflow section
 * The copyAndPopulateWorkflowSection identifies all elements we want to copy for each Workflow.
 * It then copies each element and replaces relevant text and images.
 * @param {Body} body The template document body
 * @param {array} workflowsToInclude A list of Workflows to be included in the report
 * @param {map} workflowToDistrictOrFacilitiesColumnChart An object that maps the workflow name to it's districtOrFacilitiesColumnChart
 * @param {map} workflowToOpportunityGranted An object that maps the workflow name to the number of opportunities granted for that workflow
 * @param {string} startDate The start date (queried from BigQuery) (ex: '2023-02-01')
 * @param {map} workflowToMaxOpportunityGrantedLocation An object that maps the workflow name to the district or facility with the most number of opportunities granted
 */
function copyAndPopulateWorkflowSection(
  body,
  workflowsToInclude,
  workflowToDistrictOrFacilitiesColumnChart,
  workflowToOpportunityGranted,
  startDate,
  workflowToMaxOpportunityGrantedLocation
) {
  const childIdx = getIndexOfElementToReplace(
    body,
    DocumentApp.ElementType.PARAGRAPH,
    "{{workflow_name}} | Usage & Impact Report"
  );

  const totalChildren = body.getNumChildren();

  // Now, create an array of elements we want to copy
  var elementsToCopy = [];
  for (let idx = childIdx; idx !== totalChildren; idx++) {
    let child = body.getChild(idx);
    elementsToCopy.push(child);
  }

  // For each Workflow, copy the {{workflow_name}} | Usage & Impact Report section
  workflowsToInclude.forEach((workflow) => {
    // Append new line
    body.appendParagraph("");

    elementsToCopy.forEach((element) => {
      const elementCopy = element.copy();

      if (
        elementCopy.getNumChildren() > 0 &&
        elementCopy.getChild(0).getType() ===
          DocumentApp.ElementType.INLINE_IMAGE
      ) {
        let elementCopyChild = elementCopy.getChild(0).copy();
        const altTitle = elementCopyChild.getAltTitle();
        if (altTitle === "Impact Column Chart") {
          // Replace with generated chart
          body.appendImage(workflowToDistrictOrFacilitiesColumnChart[workflow]);
        } else {
          // Put back original image
          body.appendImage(elementCopyChild);
        }
      } else {
        // Replace text
        elementCopy.replaceText("{{workflow_name}}", workflow);
        elementCopy.replaceText("{{start_date}}", startDate);

        const maxOpportunityGrantedLocation =
          workflowToMaxOpportunityGrantedLocation[workflow];
        if (maxOpportunityGrantedLocation !== null) {
          let largestNumberString =
            ", with the largest number of opportunities granted in ";
          let currentElement = elementCopy.replaceText(
            "{{region_most_opps_granted}}",
            `${largestNumberString}${maxOpportunityGrantedLocation}`
          );

          if (currentElement.findText(largestNumberString) !== null) {
            const startBlueIndex =
              currentElement
                .findText(largestNumberString)
                .getEndOffsetInclusive() + 1;
            const endBlueIndex = currentElement
              .findText(maxOpportunityGrantedLocation)
              .getEndOffsetInclusive();
            var style = {};
            style[DocumentApp.Attribute.FOREGROUND_COLOR] = "#0000FF";
            currentElement
              .editAsText()
              .setAttributes(startBlueIndex, endBlueIndex, style);
          }
        } else {
          elementCopy.replaceText("{{region_most_opps_granted}}", "");
        }

        if (
          elementCopy.findText("Impact breaks down regionally as follows:") !==
          null
        ) {
          if (workflowToOpportunityGranted[workflow] === 1) {
            elementCopy.replaceText("{{people have}}", "person has");
          } else {
            elementCopy.replaceText("{{people have}}", "people have");
          }
        }

        elementCopy.replaceText(
          "{{total_transferred}}",
          workflowToOpportunityGranted[workflow].toLocaleString()
        );

        body.appendParagraph(elementCopy);
      }
    });
  });

  // Finally, delete the original elements that we copied
  elementsToCopy.forEach((element) => {
    body.removeChild(element);
  });
}
