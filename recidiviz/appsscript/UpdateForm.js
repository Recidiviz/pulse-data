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
/* File to update the Automated Leadership Report Google Form Questions. */

const form = FormApp.openById("13fUGObQutXrluX0IRDwRyQaueWbpOlawrh36nfty9tg");
const sheet = SpreadsheetApp.openById(
  "1LW_wd4IzFXwUHgGSVAE2NCUHgNjccAAw_dUkg3qcXGE"
).getSheets()[0];

/**
 * Update google form
 * This is the entry point of the script. The updateGoogleForm function deletes the existing
 * items in the Google Form. It then builds/populates the Google Form Questions
 * using the data from the Google Sheet.
 */
function updateGoogleForm() {
  deleteFormItems();
  populateFormOptions();
}

/**
 * Delete form items
 * Gets all items in the Google Form and deletes each item.
 */
function deleteFormItems() {
  const allItems = form.getItems();

  // First, delete non-page break items
  // Without this, Apps Script throws an error when attempting to
  // delete a page break item that contains other items
  allItems.forEach((itemToDelete) => {
    if (itemToDelete.getType() !== FormApp.ItemType.PAGE_BREAK) {
      form.deleteItem(itemToDelete);
    }
  });

  // Then, delete page break items
  const pageBreakItems = form.getItems();
  pageBreakItems.forEach((e) => form.deleteItem(e));
}

/**
 * Populate form options
 * Gets a list of the unique state codes from the Google Sheet.
 * Then creates a new dropdown question in the Google Form with
 * those state codes.
 */
function populateFormOptions() {
  // First, get distinct state values
  const uniqueStates = getUniqueStates();
  // Then Populate the State dropdown
  createStateQuestion(uniqueStates);
}

/**
 * Get unique states
 * Gets a list of the unique state codes from the first column
 * in the Google Sheet.
 */
function getUniqueStates() {
  const lastRowIdx = sheet.getLastRow();

  const stateRange = sheet.getRange(`A4:A${lastRowIdx}`);

  const stateRangeValues = stateRange.getValues();
  const stateValues = stateRangeValues.map((value) => value[0]);

  const uniqueStateValues = Array.from(new Set(stateValues));

  return uniqueStateValues;
}

/**
 * Create state question
 * Creates a new State dropdown question in the Google Form
 * and populates the dropdown with the list of unique state codes.
 * @param {array} currentStates An array of unique state codes
 */
function createStateQuestion(currentStates) {
  const stateText = "State";
  const stateItem = form.addListItem();
  stateItem.setTitle(stateText);
  stateItem.setRequired(true);

  const timePeriodText = "Time Period";
  const timePeriodItem = form.addListItem();
  timePeriodItem.setTitle(timePeriodText);
  timePeriodItem.setChoiceValues(["MONTH", "QUARTER", "YEAR"]);
  timePeriodItem.setRequired(true);

  const getListOfAllowedDates = () => {
    const TODAY = new Date();
    const startOfRange = new Date(TODAY.getFullYear(), TODAY.getMonth() - 2, 1);
    const listOfAllowedDates = [];

    for (
      let date = new Date(TODAY.getFullYear(), TODAY.getMonth(), 1);
      date >= startOfRange;
      date.setMonth(date.getMonth() - 1)
    )
      listOfAllowedDates.push(date.toISOString().substring(0, 10));

    return listOfAllowedDates;
  };

  form
    .addListItem()
    .setTitle("End Date")
    .setChoiceValues(getListOfAllowedDates())
    .setRequired(true);

  const isTestReportItem = form.addMultipleChoiceItem();
  isTestReportItem
    .setTitle("Is this a TEST report?")
    .setHelpText(
      "If this is a TEST report, it will be added to the test folder."
    )
    .setChoices([
      isTestReportItem.createChoice("Yes"),
      isTestReportItem.createChoice("No"),
    ])
    .setRequired(true);

  const stateToNav = createStateNavItems();

  var choiceValues = [];

  currentStates.forEach((state) => {
    const nav = stateToNav[state];

    // When a user selects a state, they will
    // navigate to that states nav item
    const choice = stateItem.createChoice(state, nav);
    choiceValues.push(choice);
  });

  stateItem.setChoices(choiceValues);
  stateItem.setRequired(true);
}

/**
 * Create state nav items
 * For each unique state, creates a new navigation item.
 * Each nav item will contain choices for the Workflows launched in
 * the given state.
 * @returns {map} stateToNav A map of state codes to their nav items
 */
function createStateNavItems() {
  const allValuesDict = getSheetValues();
  const stateToNav = {};

  Object.entries(allValuesDict).forEach(([stateCode, columns]) => {
    const navTitle = `${stateCode} Workflows`;

    const page = form.addPageBreakItem().setTitle(navTitle);
    page.setGoToPage(FormApp.PageNavigationType.SUBMIT);

    const pageItem = form.addCheckboxItem();
    pageItem.setTitle(navTitle);

    var choices = [];
    columns.forEach((column) => {
      const value = column.workflow;
      const choice = pageItem.createChoice(value);
      choices.push(choice);
    });

    pageItem.setChoices(choices);
    stateToNav[stateCode] = page;
  });

  return stateToNav;
}

/**
 * Get Sheet Values
 * Gets all data (all rows and column) from the All Launched Workflows spreadsheet
 * Constructs an object that maps a stateCode to a list of objects. Each
 * sub-object represents a row in the spreadsheet
 * @returns {map} stateCodeToRows An object that maps stateCode to rows
 */
function getSheetValues() {
  const lastRowIdx = sheet.getLastRow();
  const lastColIdx = sheet.getLastColumn();

  // The first 2 rows in the sheet are a warning note to users
  // The third row in the sheet is the column title
  // Here, we get all sheet values starting at the fourth row and first column
  // We then offset the number of total rows by 3 since we want to ignore the
  // first 3 rows
  const values = sheet.getSheetValues(4, 1, lastRowIdx - 3, lastColIdx);

  var stateCodeToRows = {};

  values.forEach((value) => {
    const stateCode = value[0];
    const workflowName = value[1];
    const completionEventType = value[2];
    const system = value[3];
    const newDict = {
      workflow: workflowName,
      completionEventType: completionEventType,
      system: system,
    };

    if (Object.keys(stateCodeToRows).includes(stateCode)) {
      stateCodeToRows[stateCode].push(newDict);
    } else {
      stateCodeToRows[stateCode] = [newDict];
    }
  });

  return stateCodeToRows;
}
