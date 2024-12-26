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
  const isTestReport = itemResponses[3].getResponse() === "Yes";
  const workflowsToInclude = itemResponses[4].getResponse();

  Logger.log("stateCode: %s", stateCode);
  Logger.log("endDateString: %s", endDateString);
  Logger.log("workflowsToInclude: %s", workflowsToInclude);
  Logger.log("isTestReport: %s", isTestReport);

  const { startDate, startDateString } = getStartDate(
    stateCode,
    timePeriod,
    endDateString
  );

  const workflowToDistrictOrFacilitiesColumnChart = {};
  const workflowToOpportunityGrantedAndMauAndWau = {};
  const workflowToMaxOpportunityGrantedLocation = {};
  const workflowToSystem = {};
  const workflowToUsageAndImpactText = {};
  const workflowToUsageAndImpactChart = {};
  const workflowToMaxMarkedIneligibleLocation = {};
  const workflowToMauWauByRegionChart = {};
  const workflowToMaxMauWauLocation = {};
  const workflowToMauByWeekChart = {};
  const workflowToWauByWeekChart = {};

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

    const { mauWauByLocationData, mauWauXAxisColumn, mau, wau } =
      getMauWauByLocation(
        stateCode,
        endDateString,
        completionEventType,
        system
      );

    // If the query in getMauWauByLocation() returns zero rows, we won't add this chart to the report
    var mauWauByLocationChart = null;
    var highestMAULocations = null;
    var highestWAULocations = null;
    if (mauWauByLocationData) {
      mauWauByLocationChart = constructMauWauByLocationColumnChart(
        mauWauXAxisColumn,
        mauWauByLocationData,
        mau,
        wau
      );

      const mauByLocationData = mauWauByLocationData.map((row) => [
        row[0],
        row[1],
      ]);
      const wauByLocationData = mauWauByLocationData.map((row) => [
        row[0],
        row[2],
      ]);
      var { maxLocations, maxValue } = getMaxLocations(mauByLocationData);
      highestMAULocations = maxLocations;
      var { maxLocations, maxValue } = getMaxLocations(wauByLocationData);
      highestWAULocations = maxLocations;
    }

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

    const {
      usageAndImpactDistrictData,
      usageAndImpactXAxisColumn,
      eligibleAndViewedColumn,
      markedIneligibleColumn,
      eligibleAndNotViewedColumn,
    } = getUsageAndImpactDistrictData(
      stateCode,
      endDateString,
      completionEventType,
      system
    );

    const usageAndImpactColumnChart =
      constructUsageAndImpactDistrictColumnChart(
        usageAndImpactXAxisColumn,
        eligibleAndViewedColumn,
        markedIneligibleColumn,
        eligibleAndNotViewedColumn,
        usageAndImpactDistrictData
      );

    const usageAndImpactDistrictDataMarkedIneligible = [];
    usageAndImpactDistrictData.forEach((row) => {
      usageAndImpactDistrictDataMarkedIneligible.push([row[0], row[2]]);
    });

    var { maxLocations, maxValue } = getMaxLocations(
      usageAndImpactDistrictDataMarkedIneligible
    );
    workflowToMaxMarkedIneligibleLocation[workflow] = {
      maxLocations,
      maxValue,
    };

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

    var { maxLocations, maxValue } = getMaxLocations(supervisionDistrictData);
    workflowToMaxOpportunityGrantedLocation[workflow] = {
      maxLocations,
      maxValue,
    };

    var districtOrFacilitiesColumnChart =
      constructSupervisionDistrictColumnChart(
        workflow,
        xAxisColumn,
        yAxisColumn,
        supervisionDistrictData
      );

    const startDatePlusWeekString = getBoundsDateString(
      startDateString,
    );
    const endDatePlusWeekString = getBoundsDateString(
      endDateString
    );

    // MAU by Week Chart
    const { mauByWeekData, monthlyActiveUsers } = getMauByWeekData(
      stateCode,
      completionEventType,
      startDatePlusWeekString,
      endDatePlusWeekString
    );
    const mauByWeekColumnChart = mauByWeekData
      ? constructActiveUsersByWeekColumnChart(
          monthlyActiveUsers,
          mauByWeekData,
          "Monthly Active Users"
        )
      : null;
    workflowToMauByWeekChart[workflow] = mauByWeekColumnChart;

    // WAU by Week Chart
    const { wauByWeekData, weeklyActiveUsers } = getWauByWeekData(
      stateCode,
      startDateString,
      endDateString,
      completionEventType,
    );
    const wauByWeekColumnChart = wauByWeekData
      ? constructActiveUsersByWeekColumnChart(
          weeklyActiveUsers,
          wauByWeekData,
          "Weekly Active Users",
          ["#CA2E17"]
        )
      : null;
    workflowToWauByWeekChart[workflow] = wauByWeekColumnChart;

    const {
      distinctMonthlyActiveUsersByWorkflow,
      distinctMonthlyRegisteredUsers,
      distinctWeeklyActiveUsersByWorkflow,
      distinctWeeklyRegisteredUsers,
    } = constructMauAndWauByWorkflowText(
      stateCode,
      endDateString,
      completionEventType,
      system
    );

    workflowToDistrictOrFacilitiesColumnChart[workflow] =
      districtOrFacilitiesColumnChart;
    workflowToOpportunityGrantedAndMauAndWau[workflow] = {
      opportunityGranted,
      distinctMonthlyActiveUsersByWorkflow,
      distinctMonthlyRegisteredUsers,
      distinctWeeklyActiveUsersByWorkflow,
      distinctWeeklyRegisteredUsers,
    };
    workflowToUsageAndImpactText[workflow] = {
      almostEligible,
      eligible,
      markedIneligible,
      eligibleAndViewed,
      eligibleAndNotViewed,
    };
    workflowToUsageAndImpactChart[workflow] = usageAndImpactColumnChart;
    workflowToMauWauByRegionChart[workflow] = mauWauByLocationChart;
    workflowToMaxMauWauLocation[workflow] = {
      highestMAULocations,
      highestWAULocations,
    };
  });

  const {
    distinctMonthlyActiveUsersSupervisionTotal,
    distinctMonthlyRegisteredUsersSupervisionTotal,
    distinctMonthlyActiveUsersFacilitiesTotal,
    distinctMonthlyRegisteredUsersFacilitiesTotal,
    distinctWeeklyActiveUsersSupervisionTotal,
    distinctWeeklyRegisteredUsersSupervisionTotal,
    distinctWeeklyActiveUsersFacilitiesTotal,
    distinctWeeklyRegisteredUsersFacilitiesTotal,
  } = constructStatewideMauAndWauText(stateCode, endDateString);

  const statewideMauWauBySystem = {
    distinctMonthlyActiveUsersSupervisionTotal,
    distinctMonthlyRegisteredUsersSupervisionTotal,
    distinctMonthlyActiveUsersFacilitiesTotal,
    distinctMonthlyRegisteredUsersFacilitiesTotal,
    distinctWeeklyActiveUsersSupervisionTotal,
    distinctWeeklyRegisteredUsersSupervisionTotal,
    distinctWeeklyActiveUsersFacilitiesTotal,
    distinctWeeklyRegisteredUsersFacilitiesTotal,
  };

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
    workflowToUsageAndImpactText,
    workflowToUsageAndImpactChart,
    workflowToMaxMarkedIneligibleLocation,
    workflowToMauWauByRegionChart,
    workflowToMaxMauWauLocation,
    workflowToMauByWeekChart,
    workflowToWauByWeekChart,
    isTestReport,
    statewideMauWauBySystem
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
 * @param {map} workflowToSystem An object that maps the workflow name to its system ('SUPERVISION' or 'INCARCERATION')
 * @param {map} workflowToUsageAndImpactText An object that maps the workflow name to its number of people almost eligible, eligible, marked ineligible, eligible and viewed, and eligible and not viewed
 * @param {map} workflowToUsageAndImpactChart An object that maps the workflow name to its usageAndImpactColumnChart
 * @param {map} workflowToMaxMarkedIneligibleLocation An object that maps the workflow name to an object containing maxLocation (the location associated with the max number of people marked ineligible) and maxValue (the max number of people marked ineligible)
 * @param {map} workflowToMauWauByRegionChart An object that maps the workflow name to its mauWauByLocationChart
 * @param {map} workflowToMaxMauWauLocation An object that maps the worfklow name to highestMAULocations and highestWAULocations
 * @param {map} workflowToMauByWeekChart An object that maps the workflow name to its mauByWeekColumnChart
 * @param {map} workflowToWauByWeekChart An object that maps the workflow name to its wauByWeekColumnChart
 * @param {map} statewideMauWauBySystem An object that contains distinctMonthlyActiveUsersSupervisionTotal distinctMonthlyRegisteredUsersSupervisionTotal, distinctMonthlyActiveUsersFacilitiesTotal, distinctMonthlyRegisteredUsersFacilitiesTotal, distinctWeeklyActiveUsersSupervisionTotal, distinctWeeklyRegisteredUsersSupervisionTotal, distinctWeeklyActiveUsersFacilitiesTotal, and distinctWeeklyRegisteredUsersFacilitiesTotal
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
  workflowToUsageAndImpactText,
  workflowToUsageAndImpactChart,
  workflowToMaxMarkedIneligibleLocation,
  workflowToMauWauByRegionChart,
  workflowToMaxMauWauLocation,
  workflowToMauByWeekChart,
  workflowToWauByWeekChart,
  isTestReport,
  statewideMauWauBySystem
) {
  const template = DriveApp.getFileById(
    "1nsc_o2fTlldTQavxJveucWgDKkic_clKZjn0GuyF2N8"
  );
  const testFolder = DriveApp.getFolderById(
    "1oY2zratF9o3lk1Lmj03Y_Jgfwxst85Sf"
  );
  const mainFolder = DriveApp.getFolderById(
    "1UzFS4GsbgIqoLBUv0Z314-51pxQznHaJ"
  );
  const destinationFolder = isTestReport ? testFolder : mainFolder;

  const today = Utilities.formatDate(new Date(), "GMT-5", "MM/dd/yyyy");

  const copy = template.makeCopy(
    `${
      isTestReport ? "[TEST] " : ""
    }${stateCode} ${timePeriod.toLowerCase()}ly Impact Report ending ${endDateString}`,
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
  body.replaceText("{{time_period}}", timePeriod.toLowerCase());

  const exclusiveEndDate = new Date(endDateString);
  exclusiveEndDate.setDate(exclusiveEndDate.getDate() - 1);
  const endDateClean = cleanDate(
    Utilities.formatDate(exclusiveEndDate, "GMT-5", "yyyy-MM-dd")
  );
  const timeRange = `${startDate}-${endDateClean}`;
  body.replaceText("{{time_range}}", timeRange);

  const workflowToMauWauNumAndPercent = copyAndPopulateOpportunityGrants(
    body,
    workflowToOpportunityGrantedAndMauAndWau,
    workflowToSystem,
    statewideMauWauBySystem
  );

  copyAndPopulateWorkflowSection(
    body,
    workflowToSystem,
    workflowsToInclude,
    workflowToDistrictOrFacilitiesColumnChart,
    workflowToOpportunityGrantedAndMauAndWau,
    startDate,
    workflowToMaxOpportunityGrantedLocation,
    workflowToUsageAndImpactText,
    endDateClean,
    workflowToUsageAndImpactChart,
    workflowToMauWauNumAndPercent,
    workflowToMaxMarkedIneligibleLocation,
    workflowToMauWauByRegionChart,
    workflowToMaxMauWauLocation,
    workflowToMauByWeekChart,
    workflowToWauByWeekChart
  );

  insertPageBreaks(doc.getBody());
}

/**
 * For each Workflow, copy the placeholder row and populate with the Workflow name, number of Opportunities Granted, and MAU
 * @param {Body} child The child table to add rows to
 * @param {Number} templateRowIdx The number of rows in the table before adding the new rows
 * @param {map} workflowToOpportunityGrantedAndMauAndWau An object that maps the workflow name to the number of opportunities granted, the number of distinct monthly and weekly active users, and the number of distinct monthly and weekly registered users for that workflow
 */
function addWorkflowsRows(
  child,
  templateRowIdx,
  workflowToOpportunityGrantedAndMauAndWau
) {
  var newRow = null;
  var newRowIdx = templateRowIdx + 1;

  for (let [workflow, opportunityGrantedAndMauAndWau] of Object.entries(
    workflowToOpportunityGrantedAndMauAndWau
  )) {
    const rowToCopy = child.getChild(templateRowIdx).copy();
    newRow = child.insertTableRow(newRowIdx, rowToCopy);
    newRowIdx += 1;
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
      calculateActiveUsersPercent(
        opportunityGrantedAndMauAndWau.distinctMonthlyActiveUsersByWorkflow,
        opportunityGrantedAndMauAndWau.distinctMonthlyRegisteredUsers
      )
    );
    wauTextCopy.replaceText(
      "{{wau}}",
      calculateActiveUsersPercent(
        opportunityGrantedAndMauAndWau.distinctWeeklyActiveUsersByWorkflow,
        opportunityGrantedAndMauAndWau.distinctWeeklyRegisteredUsers
      )
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

  // Once we have copied all Workflows rows, we can delete the placeholder row
  child.removeRow(2);
}

/**
 * Copy and populate opportunity grants
 * Identifies, copies, and populates the total number of opportunuties granted (over all workflows) as well as the number of opportunities granted for each workflow.
 * @param {Body} body The template document body
 * @param {map} workflowToOpportunityGrantedAndMauAndWau An object that maps the workflow name to the number of opportunities granted, the number of distinct monthly and weekly active users, and the number of distinct monthly and weekly registered users for that workflow
 * @param {map} workflowToSystem An object that maps the workflow name to its system ('SUPERVISION' or 'INCARCERATION')
 * @param {map} statewideMauWauBySystem An object that contains distinctMonthlyActiveUsersSupervisionTotal distinctMonthlyRegisteredUsersSupervisionTotal, distinctMonthlyActiveUsersFacilitiesTotal, distinctMonthlyRegisteredUsersFacilitiesTotal, distinctWeeklyActiveUsersSupervisionTotal, distinctWeeklyRegisteredUsersSupervisionTotal, distinctWeeklyActiveUsersFacilitiesTotal, and distinctWeeklyRegisteredUsersFacilitiesTotal
 * @returns {map} workflowToMauWauNumAndPercent An object that maps the workflow name to its distinctMonthlyRegisteredUsers, distinctMonthlyActiveUsersByWorkflow, distinctWeeklyActiveUsersByWorkflow, workflowPercentMAU, and workflowPercentWAU
 */
function copyAndPopulateOpportunityGrants(
  body,
  workflowToOpportunityGrantedAndMauAndWau,
  workflowToSystem,
  statewideMauWauBySystem
) {
  const workflowToMauWauNumAndPercent = {};

  // First, populate the sum of all Opportunities Granted, MAU, and WAU (for all Workflows)
  var totalSupervisionOpportunitiesGranted = 0;
  var totalFacilitiesOpportunitiesGranted = 0;
  var numSupervisionWorkflows = 0;
  var numFacilitiesWorkflows = 0;

  Object.entries(workflowToOpportunityGrantedAndMauAndWau).forEach(
    ([workflow, opportunityGrantedAndMauAndWau]) => {
      const distinctMonthlyActiveUsersByWorkflow =
        opportunityGrantedAndMauAndWau.distinctMonthlyActiveUsersByWorkflow;
      const distinctMonthlyRegisteredUsers =
        opportunityGrantedAndMauAndWau.distinctMonthlyRegisteredUsers;
      const distinctWeeklyActiveUsersByWorkflow =
        opportunityGrantedAndMauAndWau.distinctWeeklyActiveUsersByWorkflow;
      const distinctWeeklyRegisteredUsers =
        opportunityGrantedAndMauAndWau.distinctWeeklyRegisteredUsers;

      const workflowPercentMAU = calculateActiveUsersPercent(
        distinctMonthlyActiveUsersByWorkflow,
        distinctMonthlyRegisteredUsers
      );
      const workflowPercentWAU = calculateActiveUsersPercent(
        distinctWeeklyActiveUsersByWorkflow,
        distinctWeeklyRegisteredUsers
      );

      workflowToMauWauNumAndPercent[workflow] = {
        distinctMonthlyRegisteredUsers,
        distinctMonthlyActiveUsersByWorkflow,
        distinctWeeklyActiveUsersByWorkflow,
        workflowPercentMAU,
        workflowPercentWAU,
      };

      if (workflowToSystem[workflow] === "SUPERVISION") {
        totalSupervisionOpportunitiesGranted +=
          opportunityGrantedAndMauAndWau.opportunityGranted;
        numSupervisionWorkflows += 1;
      } else if (workflowToSystem[workflow] === "INCARCERATION") {
        totalFacilitiesOpportunitiesGranted +=
          opportunityGrantedAndMauAndWau.opportunityGranted;
        numFacilitiesWorkflows += 1;
      }
    }
  );

  totalSupervisionOpportunitiesGranted =
    totalSupervisionOpportunitiesGranted.toLocaleString();
  totalFacilitiesOpportunitiesGranted =
    totalFacilitiesOpportunitiesGranted.toLocaleString();

  const totalSupervisionMAU = calculateActiveUsersPercent(
    statewideMauWauBySystem.distinctMonthlyActiveUsersSupervisionTotal,
    statewideMauWauBySystem.distinctMonthlyRegisteredUsersSupervisionTotal
  );
  const totalFacilitiesMAU = calculateActiveUsersPercent(
    statewideMauWauBySystem.distinctMonthlyActiveUsersFacilitiesTotal,
    statewideMauWauBySystem.distinctMonthlyRegisteredUsersFacilitiesTotal
  );
  const totalSupervisionWAU = calculateActiveUsersPercent(
    statewideMauWauBySystem.distinctWeeklyActiveUsersSupervisionTotal,
    statewideMauWauBySystem.distinctWeeklyRegisteredUsersSupervisionTotal
  );
  const totalFacilitiesWAU = calculateActiveUsersPercent(
    statewideMauWauBySystem.distinctWeeklyActiveUsersFacilitiesTotal,
    statewideMauWauBySystem.distinctWeeklyRegisteredUsersFacilitiesTotal
  );

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

  /**
   * Filters the workflow map to only include workflows matching the given system
   * @param {Object} workflowToOpportunityGrantedAndMauAndWau Map of workflow names to their opportunity/MAU/WAU data
   * @param {Object} workflowToSystem Map of workflow names to their system type ('SUPERVISION' or 'INCARCERATION')
   * @param {string} system The system type to filter for ('SUPERVISION' or 'INCARCERATION')
   * @returns {Object} Filtered map containing only workflows matching the system
   */
  const filterWorkflowsBySystem = (
    workflowToOpportunityGrantedAndMauAndWau,
    workflowToSystem,
    system
  ) =>
    Object.fromEntries(
      Object.entries(workflowToOpportunityGrantedAndMauAndWau).filter(
        ([workflow, _data]) => workflowToSystem[workflow] === system
      )
    );

  // Next, populate the Opportunities Granted for each individual Workflow

  const supervisionChild = body.getChild(
    getIndexOfElementToReplace(
      body,
      DocumentApp.ElementType.TABLE,
      "{{num_grants_pp}}"
    )
  );

  // Workflows Template Row Index
  var templateRowIdx = 2;

  // GET PP ROWS
  body.replaceText("{{num_grants_pp}}", totalSupervisionOpportunitiesGranted);
  body.replaceText("{{mau_pp}}", totalSupervisionMAU);
  body.replaceText("{{wau_pp}}", totalSupervisionWAU);

  const facilitiesChild = body.getChild(
    getIndexOfElementToReplace(
      body,
      DocumentApp.ElementType.TABLE,
      "{{num_grants_facility}}"
    )
  );

  body.replaceText(
    "{{num_grants_facility}}",
    totalFacilitiesOpportunitiesGranted
  );
  body.replaceText("{{mau_facility}}", totalFacilitiesMAU);
  body.replaceText("{{wau_facility}}", totalFacilitiesWAU);

  addWorkflowsRows(
    supervisionChild,
    templateRowIdx,
    filterWorkflowsBySystem(
      workflowToOpportunityGrantedAndMauAndWau,
      workflowToSystem,
      "SUPERVISION"
    )
  );

  addWorkflowsRows(
    facilitiesChild,
    templateRowIdx,
    filterWorkflowsBySystem(
      workflowToOpportunityGrantedAndMauAndWau,
      workflowToSystem,
      "INCARCERATION"
    )
  );

  return workflowToMauWauNumAndPercent;
}

/**
 * Copy and populate workflow section
 * The copyAndPopulateWorkflowSection identifies all elements we want to copy for each Workflow.
 * It then copies each element and replaces relevant text and images.
 * @param {Body} body The template document body
 * @param {map} workflowToSystem An object that maps the workflow name to its system ('SUPERVISION' or 'INCARCERATION')
 * @param {array} workflowsToInclude A list of Workflows to be included in the report
 * @param {map} workflowToDistrictOrFacilitiesColumnChart An object that maps the workflow name to its districtOrFacilitiesColumnChart
 * @param {map} workflowToOpportunityGrantedAndMauAndWau An object that maps the workflow name to the number of opportunities granted, the number of distinct monthly and weekly active users, and the number of distinct monthly and weekly registered users for that workflow
 * @param {string} startDate The start date (queried from BigQuery) (ex: '2023-02-01')
 * @param {map} workflowToMaxOpportunityGrantedLocation An object that maps the workflow name to the district or facility with the most number of opportunities granted
 * @param {map} workflowToUsageAndImpactText An object that maps the workflow name to its number of people almost eligible, eligible, marked ineligible, eligible and viewed, and eligible and not viewed
 * @param {string} endDateClean The end date (provided by the user via Google Form) (ex: '2023-03-01')
 * @param {map} workflowToUsageAndImpactChart An object that maps the workflow name to its usageAndImpactColumnChart
 * @param {map} workflowToMauWauNumAndPercent An object that maps the workflow name to its distinctMonthlyRegisteredUsers, distinctMonthlyActiveUsersByWorkflow, distinctWeeklyActiveUsersByWorkflow, workflowPercentMAU, and workflowPercentWAU
 * @param {map} workflowToMaxMarkedIneligibleLocation An object that maps the workflow name to an object containing maxLocation (the location associated with the max number of people marked ineligible) and maxValue (the max number of people marked ineligible)
 * @param {map} workflowToMauWauByRegionChart An object that maps the workflow name to its mauWauByLocationChart
 * @param {map} workflowToMaxMauWauLocation An object that maps the worfklow name to highestMAULocations and highestWAULocations
 * @param {map} workflowToMauByWeekChart An object that maps the workflow name to its mauByWeekColumnChart
 * @param {map} workflowToWauByWeekChart An object that maps the workflow name to its wauByWeekColumnChart
 */
function copyAndPopulateWorkflowSection(
  body,
  workflowToSystem,
  workflowsToInclude,
  workflowToDistrictOrFacilitiesColumnChart,
  workflowToOpportunityGrantedAndMauAndWau,
  startDate,
  workflowToMaxOpportunityGrantedLocation,
  workflowToUsageAndImpactText,
  endDateClean,
  workflowToUsageAndImpactChart,
  workflowToMauWauNumAndPercent,
  workflowToMaxMarkedIneligibleLocation,
  workflowToMauWauByRegionChart,
  workflowToMaxMauWauLocation,
  workflowToMauByWeekChart,
  workflowToWauByWeekChart
) {
  const childIdx = getIndexOfElementToReplace(
    body,
    DocumentApp.ElementType.PARAGRAPH,
    "{{workflow_system}} Assistant | {{workflow_name}}"
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
        
        if (altTitle === "Region Variation in Usage Chart") {
          // Replace with generated chart
          if (workflowToMauWauByRegionChart[workflow]) {
            let img = body.appendImage(workflowToMauWauByRegionChart[workflow]);
            img.setWidth(639).setHeight(455);
          }
        } else if (altTitle === "Impact Column Chart") {
          // Replace with generated chart
          if (workflowToDistrictOrFacilitiesColumnChart[workflow]) {
            let img = body.appendImage(
              workflowToDistrictOrFacilitiesColumnChart[workflow]
            );
            img.setWidth(639).setHeight(455);
          }
        } else if (altTitle === "Impact Column Chart by District or Region") {
          // Replace with generated chartk
          if (workflowToUsageAndImpactChart[workflow]) {
            let img = body.appendImage(workflowToUsageAndImpactChart[workflow]);
            img.setWidth(639).setHeight(455);
          }
        } else if (altTitle === "MAU Chart by Week") {
          // Replace with generated chart
          if (workflowToMauByWeekChart[workflow]) {
            let img = body.appendImage(workflowToMauByWeekChart[workflow]);
            img.setWidth(639).setHeight(455);
          }
        } else if (altTitle === "WAU Chart by Week") {
          // Replace with generated chart
          if (workflowToWauByWeekChart[workflow]) {
            let img = body.appendImage(workflowToWauByWeekChart[workflow]);
            img.setWidth(639).setHeight(455);
          }
        } else {
          // Put back original image
          body.appendImage(elementCopyChild);
        }
      } else {
        // Replace text
        const workflowSystem =
          workflowToSystem[workflow] === "INCARCERATION"
            ? "Facilities"
            : "Supervision";
        elementCopy.replaceText("{{workflow_system}}", workflowSystem);
        elementCopy.replaceText("{{workflow_name}}", workflow);
        elementCopy.replaceText("{{start_date}}", startDate);
        elementCopy.replaceText("{{end_date}}", endDateClean);

        elementCopy.replaceText(
          "{{num_monthly_registered_users}}",
          workflowToMauWauNumAndPercent[workflow].distinctMonthlyRegisteredUsers
        );
        elementCopy.replaceText(
          "{{num_mau}}",
          workflowToMauWauNumAndPercent[workflow]
            .distinctMonthlyActiveUsersByWorkflow
        );
        elementCopy.replaceText(
          "{{percent_mau}}",
          workflowToMauWauNumAndPercent[workflow].workflowPercentMAU
        );
        elementCopy.replaceText(
          "{{num_wau}}",
          workflowToMauWauNumAndPercent[workflow]
            .distinctWeeklyActiveUsersByWorkflow
        );
        elementCopy.replaceText(
          "{{percent_wau}}",
          workflowToMauWauNumAndPercent[workflow].workflowPercentWAU
        );

        let highestMAULocationsText = "";
        let highestWAULocationsText = "";
        if (workflowToMaxMauWauLocation[workflow].highestMAULocations) {
          highestMAULocationsText =
            workflowToMaxMauWauLocation[workflow].highestMAULocations.join(
              " and "
            );
        }
        if (workflowToMaxMauWauLocation[workflow].highestWAULocations) {
          highestWAULocationsText =
            workflowToMaxMauWauLocation[workflow].highestWAULocations.join(
              " and "
            );
        }

        if (
          workflowToMaxMauWauLocation[workflow].highestMAULocations &&
          workflowToMaxMauWauLocation[workflow].highestWAULocations
        ) {
          elementCopy.replaceText(
            "{{highest_usage_text}}",
            `Usage is highest in ${highestMAULocationsText} for MAU and ${highestWAULocationsText} for WAU`
          );
        } else if (workflowToMaxMauWauLocation[workflow].highestMAULocations) {
          elementCopy.replaceText(
            "{{highest_usage_text}}",
            `Usage is highest in ${highestMAULocationsText} for MAU`
          );
        } else if (workflowToMaxMauWauLocation[workflow].highestWAULocations) {
          elementCopy.replaceText(
            "{{highest_usage_text}}",
            `Usage is highest in ${highestWAULocationsText} for WAU`
          );
        } else {
          elementCopy.replaceText("{{highest_usage_text}}", "");
        }

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

        const largestLocationTextToReplace =
          "{{, with the largest number of people marked ineligible in {{region_ineligible}} [[:graph:]]with {{region_ineligible_num}}[[:graph:]]}}";

        if (
          workflowToMaxMarkedIneligibleLocation[workflow]["maxLocations"] &&
          workflowToMaxMarkedIneligibleLocation[workflow]["maxValue"] !== 0
        ) {
          const highestMarkedIneligibleText =
            workflowToMaxMarkedIneligibleLocation[workflow][
              "maxLocations"
            ].join(" and ");
          const largestLocationReplacement = `, with the largest number of people marked ineligible in ${highestMarkedIneligibleText} (with ${parseFloat(
            workflowToMaxMarkedIneligibleLocation[workflow].maxValue
          )})`;
          elementCopy.replaceText(
            largestLocationTextToReplace,
            largestLocationReplacement
          );
        } else {
          elementCopy.replaceText(largestLocationTextToReplace, "");
        }

        if (
          workflowToMaxOpportunityGrantedLocation[workflow].maxLocations &&
          workflowToMaxOpportunityGrantedLocation[workflow].maxValue !== 0
        ) {
          const maxOpportunityGrantedLocation =
            workflowToMaxOpportunityGrantedLocation[workflow].maxLocations.join(
              " and "
            );
          let largestNumberString =
            ", with the largest number of opportunities granted in ";
          let currentElement = elementCopy.replaceText(
            "{{region_most_opps_granted}}",
            `${largestNumberString}${maxOpportunityGrantedLocation}`
          );

          if (currentElement.findText(largestNumberString)) {
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

/**
 * Insert page breaks after each "{{page_break}}" text element
 * @param {Body} body the document body
 */
function insertPageBreaks(body) {
  let pageBreakText = body.findText("{{page_break}}").getElement();

  while (pageBreakText) {
    const pageBreakContainer = pageBreakText.getParent().asParagraph();
    const pageBreakContainerIndex = body.getChildIndex(pageBreakContainer);
    body.removeChild(pageBreakContainer);
    body.insertPageBreak(pageBreakContainerIndex);
    pageBreakText = body.findText("{{page_break}}")?.getElement();
  }
}
