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
const TEST_NUM_ELIGIBLE_OPPORTUNITIES = 15;
const TEST_NUM_ALMOST_ELIGIBLE_OPPORTUNITIES = 20;
const TEST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY = [
  { opportunityName: "Early Discharge", numClients: 6 },
  { opportunityName: "Supervision Level Mismatch", numClients: 2 },
  { opportunityName: "Classification Review", numClients: 5},
];
const TEST_ALMOST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY = [
  { opportunityName: "Early Discharge", numClients: 3 },
  { opportunityName: "Supervision Level Mismatch", numClients: 4 },
  { opportunityName: "Minimum Telephone Reporting", numClients: 1},
];

const BASE_INFO = {
  name: TESTING_NAME,
  emailAddress: TEST_DESTINATION_EMAIL,
  district: TESTING_DISTRICT,
  lastLogin: null,
  outliers: TEST_NUM_OUTLIERS,
  totalOpportunities: TEST_NUM_OPPORTUNITIES,
  eligibleOpportunities: TEST_NUM_ELIGIBLE_OPPORTUNITIES,
  almostEligibleOpportunities: TEST_NUM_ALMOST_ELIGIBLE_OPPORTUNITIES,
  eligibleClientsByOpportunity: TEST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY,
  almostEligibleClientsByOpportunity: TEST_ALMOST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY,
};

function testSendFacilitiesLinestaffEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Facilities Linestaff",
    EMAIL_SETTINGS,
    FACILITIES_LINESTAFF_INCLUDED_STATES,
    FACILITIES_LINESTAFF
  );
}

function testSendSupervisionLinestaffEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Supervision Linestaff",
    EMAIL_SETTINGS,
    SUPERVISION_LINESTAFF_INCLUDED_STATES,
    SUPERVISION_LINESTAFF
  );
}

function testSendSupervisorEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Supervisors",
    EMAIL_SETTINGS,
    SUPERVISOR_INCLUDED_STATES,
    SUPERVISORS
  );
}

function testRunFacilitiesLinestaffQuery() {
  testQuery_(FACILITIES_LINESTAFF_QUERY);
}

function testRunSupervisionLinestaffQuery() {
  testQuery_(SUPERVISION_LINESTAFF_QUERY);
}

function testRunSupervisorQuery() {
  testQuery_(SUPERVISOR_QUERY);
}

/**
 * Test to see if user login info outputs correctly
 */
function testGetUserLoginInfo() {
  const authToken = getAuth0Token();
  const emails = ["ryan@recidiviz.org", "kirtana@recidiviz.org"];
  const userLoginInfo = getUserLoginInfo(emails, authToken);

  if (typeof userLoginInfo !== "object") {
    throw new Error(
      `User login info is returning ${typeof userLoginInfo}, not an object.`
    );
  }

  if (Object.keys(userLoginInfo).length != emails.length) {
    throw new Error(
      `Length of user login info is ${
        Object.keys(userLoginInfo).length
      } when it should be ${
        emails.length
      }. The current emails going through are ${Object.keys(userLoginInfo)}`
    );
  }

  // Checking keys and values of userLoginInfo
  for (const [email, lastLogin] of Object.entries(userLoginInfo)) {
    if (!emails.includes(email)) {
      throw new Error(
        "The user login info keys (" + emails + ") are not valid"
      );
    }
    if (!(lastLogin instanceof Date)) {
      throw new Error(
        "User login info values (" + lastLogin + ") are not dates"
      );
    }
  }
  console.log("Successfully fetched login information:", userLoginInfo);
}

function testParseClientsByOpportunity() {
  testParseClientsByOpportunity_(SUPERVISION_LINESTAFF_QUERY);
}

// =============================================================================
// Private functions, indicated by the underscore at the end of the name, will not
// show up in the Apps Script UI.

function sendTestEmails_(sheetName, settings, stateCodesToTest, userType) {
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
    const body = buildLoginReminderBody(info, userType, testSettings);
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

function testParseClientsByOpportunity_(query) {
  console.log("Query: ");
  console.log(query);
  console.log("Running query...");
  const data = RecidivizHelpers.runQuery(query);
  if (!data) {
    console.log("Found no data.");
    return;
  }

  const sampleData = data.slice(0, 20);
  const parsedData = sampleData.map((row) => {
    stateCode = row[0];
    column = row[9];
    return parseClientsByOpportunity(stateCode, column);
  });
  console.log("Sample of parsed data:");
  console.log(parsedData);
}
