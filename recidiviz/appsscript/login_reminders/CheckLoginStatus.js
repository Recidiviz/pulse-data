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
/* Apps Script for checking whether a user has logged in based on BQ connected sheets. */

function checkLoginStatus() {
  // Open both sheets
  const userSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(
    "Email Reminders - MI, ID, TN, and ME"
  );
  const emailSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(
    "September Sent Emails"
  );

  // Get data from both sheets
  const userData = userSheet.getDataRange().getValues();
  const emailData = emailSheet.getDataRange().getValues();

  // Loop through the email data (Emails Sent sheet)
  for (let i = 1; i < emailData.length; i++) {
    const emailSent = emailData[i][2]; // Assuming 3rd column is the email address in Sent Emails sheet
    const emailSentTimestamp = new Date(emailData[i][4]); // Assuming 5th column is the email sent timestamp

    // Now, find the matching user in the User Info sheet by email
    for (let j = 1; j < userData.length; j++) {
      const userEmail = userData[j][3]; // Assuming 4th column is the email address in User Info sheet
      const loginTimestamp = userData[j][5]; // Assuming 6th column is the most_recent_login timestamp

      // If the emails match
      if (emailSent == userEmail) {
        Logger.log("Found matching user: " + userEmail);

        // Check if the user has logged in after the email was sent
        if (loginTimestamp) {
          // Check if there's a login timestamp
          const userLoginDate = new Date(loginTimestamp);

          if (userLoginDate > emailSentTimestamp) {
            Logger.log("User logged in after email sent");
            const formattedDateTime = Utilities.formatDate(
              userLoginDate,
              Session.getScriptTimeZone(),
              "MM/dd/yyyy HH:mm:ss"
            );
            emailSheet.getRange(i + 1, 6).setValue(formattedDateTime); // Assuming you want to write in the 5th column (Login Time)
          } else {
            Logger.log("No login after email sent");
          }
        } else {
          Logger.log("No login found for user");
        }
        break; // Exit the inner loop once a match is found
      }
    }
  }
}
