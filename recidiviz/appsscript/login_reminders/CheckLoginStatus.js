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
/* Apps Script for checking whether users have logged in based on auth0. */

/**
 * Checks whether the users whose information is listed in SENT_EMAILS_SHEET have logged
 * in by querying auth0.
 *
 * If a user logged in after the email send timestamp, writes their
 * MOST RECENT login timestamp (as of when this script was run) to the sheet.
 *
 * Prints a summary of how many users logged in after email send.
 */
function checkLoginStatus() {
  const authToken = getAuth0Token();

  // The first two sheets returned by getSheets() are the leftmost in viewing order.
  // We assume these were the most recently added by sending email reminders
  const sheets = SpreadsheetApp.getActiveSpreadsheet().getSheets().slice(0, 2);
  for (const sheet of sheets) {
    console.log(`Getting login statuses for sheet "${sheet.getName()}"`);
    writeLoginStatusToSheet_(sheet, authToken);
  }
}

/**
 * Private function for writing login status to a particular spreadsheet.
 * @param {Spreadsheet} sheet the spreadsheet we'd like to write to
 * @param {string} authToken  an auth0 Management API Token
 */
function writeLoginStatusToSheet_(sheet, authToken) {
  // Ignoring the first row, get all the emails and email sent timestamps,
  // which we assume are in columns 3 and 5
  const emailData = sheet.getDataRange();
  const emailToRow = Object.fromEntries(
    emailData
      .getValues()
      .slice(1)
      .map((row, i) => [
        row[2].toLowerCase(), // email address
        {
          rowIndex: i,
          emailSentTimestamp: row[4],
        },
      ])
  );

  // Query auth0 for login data for all users who were emailed
  const allEmails = Object.keys(emailToRow);
  const loginInfo = getUserLoginInfo(allEmails, authToken);
  let loginsAfterEmailSent = 0;
  const colToWrite = emailData.getValues()[0].length + 1;
  for (const [email, lastLogin] of Object.entries(loginInfo)) {
    const { rowIndex, emailSentTimestamp } = emailToRow[email];
    if (lastLogin > new Date(emailSentTimestamp)) {
      loginsAfterEmailSent++;
      // Write the login time to the 6th column of the sheet
      // (rows and columns for getRange are 1-indexed)
      const formattedDateTime = lastLogin.toISOString();
      sheet.getRange(rowIndex + 1, colToWrite).setValue(formattedDateTime);
    }
  }

  // Print summary and write header to sheet
  const formattedToday = new Date().toLocaleDateString("en-US", {
    timeZone: "America/New_York",
    timeZoneName: "short",
    hour12: true,
    year: "numeric",
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
  sheet
    .getRange(1, colToWrite)
    .setValue(`Most Recent Login as of ${formattedToday}`);
  const numEmailsSent = emailData.getNumRows();
  const loginSummary = `As of ${formattedToday}, ${loginsAfterEmailSent} out of ${numEmailsSent} emailed users in "${sheet.getName()}" had logged in after the reminder email was sent to them.`;
  console.log(loginSummary);
}
