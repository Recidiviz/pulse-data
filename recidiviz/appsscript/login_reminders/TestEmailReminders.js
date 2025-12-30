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
/**
 * How to use this file:
 *
 * Manually run these functions from the Apps Script UI at
 * script.google.com/home/projects/<script ID>
 * There is a dropdown to select a function to run.
 *
 * If you want to run a test only for certain state code(s), change the list of states
 * at the top of the appropriate file Send[*]EmailReminders.gs and save that file (cmd+S).
 *
 * The testing functions:
 * - testReal[*]Emails: Runs the script to send emails to real users, but instead of
 *    actually sending the emails, writes the email text to the connected spreadsheet.
 *    These functions will go through all real users returned by the query but will only
 *    write up to TEST_EMAIL_LIMIT_PER_STATE emails per state
 *    You can use this function to test that the script works end-to-end and to check
 *    what emails will be sent to real users.
 * - testSend[*]Emails: Sends emails with fake testing data to the currently logged-in user.
 *    Usually, this is the apps-script@ email account.
 *    You can use this function to check how email HTML is formatted in Gmail.
 * - testRun[*]Query: Prints the result of the BQ query that we run to collect
 *    the list of users we might email.
 *    You can use this function to preview the query or test how data is parsed into Apps Script land.
 *
 * The remaining functions are tests for the corresponding helper functions in EmailReminderHelpers.
 * */

const TEST_DESTINATION_EMAIL = Session.getActiveUser().getEmail();

const TEST_EMAIL_LIMIT_PER_STATE = 15;

const TESTING_NAME = "Firstname Lastname";
const TESTING_DISTRICT = "Fake District";
const TEST_NUM_OUTLIERS = 123;
const TEST_NUM_OPPORTUNITIES = 456;
const TEST_NUM_ELIGIBLE_OPPORTUNITIES = 15;
const TEST_NUM_ALMOST_ELIGIBLE_OPPORTUNITIES = 20;
const TEST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY = [
  { opportunityName: "Early Discharge", numClients: 6 },
  { opportunityName: "Supervision Level Mismatch", numClients: 2 },
  { opportunityName: "Classification Review", numClients: 5 },
];
const TEST_ALMOST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY = [
  { opportunityName: "Early Discharge", numClients: 3 },
  { opportunityName: "Supervision Level Mismatch", numClients: 4 },
  { opportunityName: "Minimum Telephone Reporting", numClients: 1 },
];
const TEST_URGENT_CLIENTS_BY_OPPORTUNITY = [
  { opportunityName: "Early Discharge", numClients: 2 },
  { opportunityName: "Classification Review", numClients: 3 },
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
  almostEligibleClientsByOpportunity:
    TEST_ALMOST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY,
  urgentClientsByOpportunity: [],
};

const TEST_USERS = [
  {
    testName: "NO_LOGIN_NO_URGENT",
    ...BASE_INFO,
  },
  {
    testName: "NO_LOGIN_URGENT",
    name: TESTING_NAME,
    emailAddress: TEST_DESTINATION_EMAIL,
    district: TESTING_DISTRICT,
    lastLogin: null,
    outliers: TEST_NUM_OUTLIERS,
    totalOpportunities: TEST_NUM_OPPORTUNITIES,
    eligibleOpportunities: TEST_NUM_ELIGIBLE_OPPORTUNITIES,
    almostEligibleOpportunities: TEST_NUM_ALMOST_ELIGIBLE_OPPORTUNITIES,
    eligibleClientsByOpportunity: TEST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY,
    almostEligibleClientsByOpportunity:
      TEST_ALMOST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY,
    urgentClientsByOpportunity: TEST_URGENT_CLIENTS_BY_OPPORTUNITY,
  },
  {
    testName: "LOGIN_URGENT",
    name: TESTING_NAME,
    emailAddress: TEST_DESTINATION_EMAIL,
    district: TESTING_DISTRICT,
    lastLogin: new Date(),
    outliers: TEST_NUM_OUTLIERS,
    totalOpportunities: TEST_NUM_OPPORTUNITIES,
    eligibleOpportunities: TEST_NUM_ELIGIBLE_OPPORTUNITIES,
    almostEligibleOpportunities: TEST_NUM_ALMOST_ELIGIBLE_OPPORTUNITIES,
    eligibleClientsByOpportunity: TEST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY,
    almostEligibleClientsByOpportunity:
      TEST_ALMOST_ELIGIBLE_CLIENTS_BY_OPPORTUNITY,
    urgentClientsByOpportunity: TEST_URGENT_CLIENTS_BY_OPPORTUNITY,
  },
];

const SUPERVISION_LINESTAFF_USER_MAPPING = {
  US_IX: "NO_LOGIN_NO_URGENT",
  US_ME: "NO_LOGIN_NO_URGENT",
  US_MI: "NO_LOGIN_URGENT",
  US_NE: "NO_LOGIN_NO_URGENT",
  US_ND: "NO_LOGIN_URGENT",
  US_NE: "NO_LOGIN_NO_URGENT",
  US_PA: "LOGIN_URGENT",
  US_TN: "LOGIN_URGENT",
  US_TX: "NO_LOGIN_URGENT",
  US_UT: "NO_LOGIN_URGENT",
};

const TEST_SETTINGS = {
  ...EMAIL_SETTINGS,
  IS_TESTING: true,
};

function testRealFacilitiesLinestaffEmails() {
  sendAllLoginReminders(
    FACILITIES_LINESTAFF,
    FACILITIES_LINESTAFF_QUERY,
    TEST_SETTINGS,
    FACILITIES_LINESTAFF_INCLUDED_STATES
  );
}

function testRealSupervisionLinestaffEmails() {
  sendAllLoginReminders(
    SUPERVISION_LINESTAFF,
    SUPERVISION_LINESTAFF_QUERY,
    TEST_SETTINGS,
    SUPERVISION_LINESTAFF_INCLUDED_STATES
  );
}

function testRealSupervisorsEmails() {
  sendAllLoginReminders(
    SUPERVISORS,
    SUPERVISOR_QUERY,
    TEST_SETTINGS,
    SUPERVISOR_INCLUDED_STATES
  );
}

function testSendFacilitiesLinestaffEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Facilities Linestaff",
    TEST_SETTINGS,
    FACILITIES_LINESTAFF_INCLUDED_STATES,
    FACILITIES_LINESTAFF
  );
}

function testSendSupervisionLinestaffEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Supervision Linestaff",
    TEST_SETTINGS,
    SUPERVISION_LINESTAFF_INCLUDED_STATES,
    SUPERVISION_LINESTAFF,
    SUPERVISION_LINESTAFF_USER_MAPPING
  );
}

function testSendSupervisorEmails() {
  sendTestEmails_(
    "[TESTING] Sent Emails to Supervisors",
    TEST_SETTINGS,
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

function sendTestEmails_(
  sheetName,
  settings,
  stateCodesToTest,
  userType,
  userMapping = null
) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet();
  let sentEmailsSheet = sheet.getSheetByName(sheetName);
  if (!sentEmailsSheet) {
    sentEmailsSheet =
      SpreadsheetApp.getActiveSpreadsheet().insertSheet(sheetName);
  }

  for (stateCode of stateCodesToTest) {
    let userInfo;
    if (userMapping) {
      if (userMapping[stateCode]) {
        userInfo = TEST_USERS.find(
          (user) => user.testName === userMapping[stateCode]
        );
      } else {
        throw new Error(
          `State "${stateCode}" not found in userMapping. Please specify a user for this state.`
        );
      }
    } else {
      userInfo = BASE_INFO;
    }

    const info = { ...userInfo, stateCode };
    const body = buildLoginReminderBody(info, userType, settings);
    sendLoginReminder(info, body, sentEmailsSheet, settings);
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
