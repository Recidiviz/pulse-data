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
/* Tests sending email reminders - each of these functions will send the emails 
   to DESTINATION_EMAIL and write to a new sheet in the connected spreadsheet. */

// Testing emails will be sent to the currently logged-in user.
const TEST_DESTINATION_EMAIL = Session.getActiveUser().getEmail();

const TESTING_NAME = "Firstname Lastname";
const TESTING_DISTRICT = "Fake District";
const TEST_NUM_OUTLIERS = 123;
const TEST_NUM_OPPORTUNITIES = 456;

const BASE_INFO = {
  name: TESTING_NAME,
  emailAddress: TEST_DESTINATION_EMAIL,
  district: TESTING_DISTRICT,
  lastLogin: null,
  outliers: TEST_NUM_OUTLIERS,
  opportunities: TEST_NUM_OPPORTUNITIES,
};

function testSendLinestaffEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Linestaff",
    LINESTAFF_SETTINGS,
    LINESTAFF_INCLUDED_STATES,
    false
  );
}

function testSendSupervisorEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Supervisors",
    SUPERVISOR_SETTINGS,
    SUPERVISOR_INCLUDED_STATES,
    true
  );
}

function testRunLinestaffQuery() {
  testQuery_(LINESTAFF_QUERY);
}

function testRunSupervisorQuery() {
  testQuery_(SUPERVISOR_QUERY);
}

// =============================================================================
// Private functions, indicated by the underscore at the end of the name, will not
// show up in the Apps Script UI.

function sendTestEmails_(sheetName, settings, stateCodesToTest, isSupervisors) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet();
  let sentEmailsSheet = sheet.getSheetByName(sheetName);
  if (!sentEmailsSheet) {
    sentEmailsSheet =
      SpreadsheetApp.getActiveSpreadsheet().insertSheet(sheetName);
  }

  for (stateCode of stateCodesToTest) {
    const testSettings = {
      ...settings,
      EMAIL_SUBJECT: `[TESTING ${stateCode}] ${settings.EMAIL_SUBJECT}`,
    };
    const info = { ...BASE_INFO, stateCode };
    const body = buildLoginReminderBody(info, isSupervisors, testSettings);
    sendLoginReminder(info, body, sentEmailsSheet, testSettings);
  }
  console.log(
    `Sent test emails to ${TEST_DESTINATION_EMAIL} and logged sent emails in spreadsheet "${sheetName}"`
  );
}

function testQuery_(query) {
  console.log("Query: ");
  console.log(query);
  console.log("Running query...");
  const data = RecidivizHelpers.runQuery(query);
  if (!data) {
    console.log("Found no data.");
    return;
  }
  console.log(
    `Found ${data.length} users with opportunities/outliers. Sample:`
  );
  console.log(data.slice(0, 20));
}
