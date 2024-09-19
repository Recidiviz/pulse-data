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

const projectId = "recidiviz-123";

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
  const workflowToOpportunityGrantedAndMauAndWau = {};
  const workflowToMaxOpportunityGrantedLocation = {};
  const workflowToSystem = {};
  const workflowToUsageAndImpactText = {};

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
    workflowToSystem[workflow] = system;

    const {
      almostEligible,
      eligible,
      markedIneligible,
      eligibleAndViewed,
      eligibleAndNotViewed,
    } = constructUsageAndImpactText(
      stateCode,
      endDateString,
      completionEventType
    );

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

    const {
      distinctMonthlyActiveUsers,
      distinctMonthlyRegisteredUsers,
      distinctWeeklyActiveUsers,
      distinctWeeklyRegisteredUsers,
    } = constructMauAndWauText(stateCode, endDateString, completionEventType);

    workflowToDistrictOrFacilitiesColumnChart[workflow] =
      districtOrFacilitiesColumnChart;
    workflowToOpportunityGrantedAndMauAndWau[workflow] = {
      opportunityGranted,
      distinctMonthlyActiveUsers,
      distinctMonthlyRegisteredUsers,
      distinctWeeklyActiveUsers,
      distinctWeeklyRegisteredUsers,
    };
    workflowToMaxOpportunityGrantedLocation[workflow] = maxRegion;
    workflowToUsageAndImpactText[workflow] = {
      almostEligible,
      eligible,
      markedIneligible,
      eligibleAndViewed,
      eligibleAndNotViewed,
    };
  });

  copyAndPopulateTemplateDoc(
    workflowToDistrictOrFacilitiesColumnChart,
    stateCode,
    timePeriod,
    endDateString,
    workflowsToInclude,
    workflowToOpportunityGrantedAndMauAndWau,
    startDate,
    workflowToMaxOpportunityGrantedLocation,
    workflowToSystem,
    workflowToUsageAndImpactText
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
 * @param {map} workflowToOpportunityGrantedAndMauAndWau An object that maps the workflow name to the number of opportunities granted, the number of distinct monthly and weekly active users, and the number of distinct monthly and weekly registered users for that workflow
 * @param {string} startDate The start date (queried from BigQuery) (ex: '2023-02-01')
 * @param {map} workflowToMaxOpportunityGrantedLocation An object that maps the workflow name to the district or facility with the most number of opportunities granted
 * @param {map} workflowToSystem An object that maps the workflow name to it's system ('SUPERVISION' or 'INCARCERATION')
 * @param {map} workflowToUsageAndImpactText An object that maps the workflow name to its number of people almost eligible, eligible, marked ineligible, eligible and viewed, and eligible and not viewed
 */
function copyAndPopulateTemplateDoc(
  workflowToDistrictOrFacilitiesColumnChart,
  stateCode,
  timePeriod,
  endDateString,
  workflowsToInclude,
  workflowToOpportunityGrantedAndMauAndWau,
  startDate,
  workflowToMaxOpportunityGrantedLocation,
  workflowToSystem,
  workflowToUsageAndImpactText
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

  copyAndPopulateOpportunityGrants(
    body,
    workflowToOpportunityGrantedAndMauAndWau,
    workflowToSystem
  );

  copyAndPopulateWorkflowSection(
    body,
    workflowsToInclude,
    workflowToDistrictOrFacilitiesColumnChart,
    workflowToOpportunityGrantedAndMauAndWau,
    startDate,
    workflowToMaxOpportunityGrantedLocation,
    workflowToUsageAndImpactText,
    endDateClean
  );
}

/**
 * Copy and populate opportunity grants
 * Identifies, copies, and populates the total number of opportunuties granted (over all workflows) as well as the number of opportunities granted for each workflow.
 * @param {Body} body The template document body
 * @param {map} workflowToOpportunityGrantedAndMauAndWau An object that maps the workflow name to the number of opportunities granted, the number of distinct monthly and weekly active users, and the number of distinct monthly and weekly registered users for that workflow
 * @param {map} workflowToSystem An object that maps the workflow name to it's system ('SUPERVISION' or 'INCARCERATION')
 */
function copyAndPopulateOpportunityGrants(
  body,
  workflowToOpportunityGrantedAndMauAndWau,
  workflowToSystem
) {
  // First, populate the sum of all Opportunities Granted, MAU, and WAU (for all Workflows)
  var totalSupervisionOpportunitiesGranted = 0;
  var totalFacilitiesOpportunitiesGranted = 0;
  var totalSupervisionMonthlyActiveUsers = 0;
  var totalSupervisionMonthlyRegisteredUsers = 0;
  var totalFacilitiesMonthlyActiveUsers = 0;
  var totalFacilitiesMonthlyRegisteredUsers = 0;
  var totalSupervisionWeeklyActiveUsers = 0;
  var totalSupervisionWeeklyRegisteredUsers = 0;
  var totalFacilitiesWeeklyActiveUsers = 0;
  var totalFacilitiesWeeklyRegisteredUsers = 0;
  var numSupervisionWorkflows = 0;
  var numFacilitiesWorkflows = 0;

  Object.entries(workflowToOpportunityGrantedAndMauAndWau).forEach(
    ([workflow, opportunityGrantedAndMauAndWau]) => {
      if (workflowToSystem[workflow] === "SUPERVISION") {
        totalSupervisionOpportunitiesGranted +=
          opportunityGrantedAndMauAndWau.opportunityGranted;
        totalSupervisionMonthlyActiveUsers +=
          opportunityGrantedAndMauAndWau.distinctMonthlyActiveUsers;
        totalSupervisionMonthlyRegisteredUsers +=
          opportunityGrantedAndMauAndWau.distinctMonthlyRegisteredUsers;
        totalSupervisionWeeklyActiveUsers +=
          opportunityGrantedAndMauAndWau.distinctWeeklyActiveUsers;
        totalSupervisionWeeklyRegisteredUsers +=
          opportunityGrantedAndMauAndWau.distinctWeeklyRegisteredUsers;
        numSupervisionWorkflows += 1;
      } else if (workflowToSystem[workflow] === "INCARCERATION") {
        totalFacilitiesOpportunitiesGranted +=
          opportunityGrantedAndMauAndWau.opportunityGranted;
        totalFacilitiesMonthlyActiveUsers +=
          opportunityGrantedAndMauAndWau.distinctMonthlyActiveUsers;
        totalFacilitiesMonthlyRegisteredUsers +=
          opportunityGrantedAndMauAndWau.distinctMonthlyRegisteredUsers;
        totalFacilitiesWeeklyActiveUsers +=
          opportunityGrantedAndMauAndWau.distinctWeeklyActiveUsers;
        totalFacilitiesWeeklyRegisteredUsers +=
          opportunityGrantedAndMauAndWau.distinctWeeklyRegisteredUsers;
        numFacilitiesWorkflows += 1;
      }
    }
  );

  totalSupervisionOpportunitiesGranted =
    totalSupervisionOpportunitiesGranted.toLocaleString();
  totalFacilitiesOpportunitiesGranted =
    totalFacilitiesOpportunitiesGranted.toLocaleString();
  const totalSupervisionMAU = calculateActiveUsersPercent(totalSupervisionMonthlyActiveUsers, totalSupervisionMonthlyRegisteredUsers);
  const totalFacilitiesMAU = calculateActiveUsersPercent(totalFacilitiesMonthlyActiveUsers, totalFacilitiesMonthlyRegisteredUsers);
  const totalSupervisionWAU = calculateActiveUsersPercent(totalSupervisionWeeklyActiveUsers, totalSupervisionWeeklyRegisteredUsers);
  const totalFacilitiesWAU = calculateActiveUsersPercent(totalFacilitiesWeeklyActiveUsers, totalFacilitiesWeeklyRegisteredUsers);

  Logger.log(
    "totalSupervisionOpportunitiesGranted: %s",
    totalSupervisionOpportunitiesGranted
  );
  Logger.log(
    "totalFacilitiesOpportunitiesGranted: %s",
    totalFacilitiesOpportunitiesGranted
  );
  Logger.log("totalSupervisionMAU: %s", totalSupervisionMAU);
  Logger.log("totalFacilitiesMAU: %s", totalFacilitiesMAU);
  Logger.log("totalSupervisionWAU: %s", totalSupervisionWAU);
  Logger.log("totalFacilitiesWAU: %s", totalFacilitiesWAU);
  Logger.log("numSupervisionWorkflows: %s", numSupervisionWorkflows);
  Logger.log("numFacilitiesWorkflows: %s", numFacilitiesWorkflows);

  // Next, populate the Opportunities Granted for each individual Workflow
  const childIdx = getIndexOfElementToReplace(
    body,
    DocumentApp.ElementType.TABLE,
    "{{num_grants_pp}}"
  );
  const child = body.getChild(childIdx);

  // To start, the table will have 5 rows as a default (rows are zero indexed)
  var numTableRows = 4;

  if (numSupervisionWorkflows > 0) {
    body.replaceText("{{num_grants_pp}}", totalSupervisionOpportunitiesGranted);
    body.replaceText("{{mau_pp}}", totalSupervisionMAU);
    body.replaceText("{{wau_pp}}", totalSupervisionWAU);
  } else {
    // remove P&P rows from table
    child.removeRow(3);
    numTableRows -= 1;
  }

  if (numFacilitiesWorkflows > 0) {
    body.replaceText(
      "{{num_grants_facility}}",
      totalFacilitiesOpportunitiesGranted
    );
    body.replaceText("{{mau_facility}}", totalFacilitiesMAU);
    body.replaceText("{{wau_facility}}", totalFacilitiesWAU);
  } else {
    // remove Facilities rows from table
    child.removeRow(numTableRows);
    numTableRows -= 1;
  }

  // For each Workflow, copy the placeholder row and populate with the Workflow name, number of Opportunities Granted, and MAU
  Object.entries(workflowToOpportunityGrantedAndMauAndWau).forEach(
    ([workflow, opportunityGrantedAndMauAndWau]) => {
      var newRow = null;
      var newRowIdx = null;
      const rowToCopy = child.getChild(2).copy();

      if (workflowToSystem[workflow] === "SUPERVISION") {
        // Add new row to table in P&P section
        newRowIdx = 4;
      } else if (workflowToSystem[workflow] === "INCARCERATION") {
        // Add new row to table in Facilities section
        newRowIdx = numTableRows + 1;
      }

      newRow = child.insertTableRow(newRowIdx, rowToCopy);
      numTableRows += 1;
      const nameCell = newRow.getCell(0);
      const opportunityGrantsCell = newRow.getCell(1);
      const mauCell = newRow.getCell(2);
      const wauCell = newRow.getCell(3);
      const nameTextToReplace = nameCell.getChild(0);
      const opportunityGrantsTextToReplace = opportunityGrantsCell.getChild(0);
      const mauTextToReplace = mauCell.getChild(0);
      const wauTextToReplace = wauCell.getChild(0);

      const nameTextCopy = nameTextToReplace.copy();
      const opportunityGrantsTextCopy = opportunityGrantsTextToReplace.copy();
      const mauTextCopy = mauTextToReplace.copy();
      const wauTextCopy = wauTextToReplace.copy();

      nameTextCopy.replaceText("{{workflow_name}}", workflow);
      opportunityGrantsTextCopy.replaceText(
        "{{num_grants}}",
        opportunityGrantedAndMauAndWau.opportunityGranted.toLocaleString()
      );
      mauTextCopy.replaceText(
        "{{mau}}",
        calculateActiveUsersPercent(opportunityGrantedAndMauAndWau.distinctMonthlyActiveUsers, opportunityGrantedAndMauAndWau.distinctMonthlyRegisteredUsers)
      );
      wauTextCopy.replaceText(
        "{{wau}}",
        calculateActiveUsersPercent(opportunityGrantedAndMauAndWau.distinctWeeklyActiveUsers, opportunityGrantedAndMauAndWau.distinctWeeklyRegisteredUsers)
      );

      nameCell.appendParagraph(nameTextCopy);
      opportunityGrantsCell.appendParagraph(opportunityGrantsTextCopy);
      mauCell.appendParagraph(mauTextCopy);
      wauCell.appendParagraph(wauTextCopy);

      // Finally, delete the original element that we copied
      nameCell.removeChild(nameTextToReplace);
      opportunityGrantsCell.removeChild(opportunityGrantsTextToReplace);
      mauCell.removeChild(mauTextToReplace);
      wauCell.removeChild(wauTextToReplace);
    }
  );

  // Once we have copied all Workflows rows, we can delete the placeholder row
  child.removeRow(2);
}

/**
 * Copy and populate workflow section
 * The copyAndPopulateWorkflowSection identifies all elements we want to copy for each Workflow.
 * It then copies each element and replaces relevant text and images.
 * @param {Body} body The template document body
 * @param {array} workflowsToInclude A list of Workflows to be included in the report
 * @param {map} workflowToDistrictOrFacilitiesColumnChart An object that maps the workflow name to it's districtOrFacilitiesColumnChart
 * @param {map} workflowToOpportunityGrantedAndMauAndWau An object that maps the workflow name to the number of opportunities granted, the number of distinct monthly and weekly active users, and the number of distinct monthly and weekly registered users for that workflow
 * @param {string} startDate The start date (queried from BigQuery) (ex: '2023-02-01')
 * @param {map} workflowToMaxOpportunityGrantedLocation An object that maps the workflow name to the district or facility with the most number of opportunities granted
 * @param {map} workflowToUsageAndImpactText An object that maps the workflow name to it's number of people almost eligible, eligible, marked ineligible, eligible and viewed, and eligible and not viewed
 * @param {string} endDateClean The end date (provided by the user via Google Form) (ex: '2023-03-01')
 */
function copyAndPopulateWorkflowSection(
  body,
  workflowsToInclude,
  workflowToDistrictOrFacilitiesColumnChart,
  workflowToOpportunityGrantedAndMauAndWau,
  startDate,
  workflowToMaxOpportunityGrantedLocation,
  workflowToUsageAndImpactText,
  endDateClean
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
        elementCopy.replaceText("{{end_date}}", endDateClean);
        elementCopy.replaceText(
          "{{num_ineligible}}",
          workflowToUsageAndImpactText[
            workflow
          ].markedIneligible.toLocaleString()
        );
        elementCopy.replaceText(
          "{{num_eligible}}",
          workflowToUsageAndImpactText[workflow].eligible.toLocaleString()
        );
        elementCopy.replaceText(
          "{{num_reviewed}}",
          workflowToUsageAndImpactText[
            workflow
          ].eligibleAndViewed.toLocaleString()
        );

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
          if (
            workflowToOpportunityGrantedAndMauAndWau[workflow]
              .opportunityGranted === 1
          ) {
            elementCopy.replaceText("{{people have}}", "person has");
          } else {
            elementCopy.replaceText("{{people have}}", "people have");
          }
        }

        elementCopy.replaceText(
          "{{total_transferred}}",
          workflowToOpportunityGrantedAndMauAndWau[
            workflow
          ].opportunityGranted.toLocaleString()
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
