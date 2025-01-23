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

const STATE_CODES_TO_TEST = ["US_IX", "US_ME", "US_MI", "US_ND", "US_TN"];

const DESTINATION_EMAIL = Session.getActiveUser().getEmail();

const TESTING_NAME = "Firstname Lastname";
const TESTING_DISTRICT = "Fake District";
const NUM_OUTLIERS = 123;
const NUM_OPPORTUNITIES = 456;

const BASE_INFO = {
  name: TESTING_NAME,
  emailAddress: DESTINATION_EMAIL,
  district: TESTING_DISTRICT,
  lastLogin: null,
  outliers: NUM_OUTLIERS,
  opportunities: NUM_OPPORTUNITIES,
};

function sendTestLinestaffEmails() {
  sendTestEmails_("[TESTING] Sent Emails to Linestaff", LINESTAFF_SETTINGS);
}

function sendTestSupervisorEmails() {
  sendTestEmails_("[TESTING] Sent Emails to Supervisors", SUPERVISOR_SETTINGS);
}

// The underscore indicates this is a private function, to remove
// the option to run it from the Apps Script UI
function sendTestEmails_(sheetName, settings) {
  const sheet = SpreadsheetApp.getActiveSpreadsheet();
  let sentEmailsSheet = sheet.getSheetByName(sheetName);
  if (!sentEmailsSheet) {
    sentEmailsSheet =
      SpreadsheetApp.getActiveSpreadsheet().insertSheet(sheetName);
  }

  for (stateCode of STATE_CODES_TO_TEST) {
    const info = { ...BASE_INFO, stateCode };
    const body = buildLoginReminderBody(info, settings);
    sendLoginReminder(info, body, sentEmailsSheet, settings);
  }
}
