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
/* Apps Script for sending email reminders to line staff. */

const EXCLUDED_DISTRICTS = ["NOT_APPLICABLE", "EXTERNAL_UNKNOWN"];

const EMAIL_FROM_ALIAS = "email-reports@recidiviz.org";
const FEEDBACK_EMAIL = "feedback@recidiviz.org";

const EMAIL_SUBJECT = "Recidiviz missed you this month!";
const RECIDIVIZ_LINK = "https://dashboard.recidiviz.org/";
const RECIDIVIZ_LINK_TEXT = "Login to Recidiviz";

function sendLinestaffEmailReminders() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(
    "Email Reminders - MI, ID, TN, ND, and ME"
  );
  const data = sheet.getDataRange().getValues();

  const now = new Date();
  const formattedDate = now.toLocaleString("en-US", {
    timeZone: "America/New_York",
    hour12: true,
    year: "numeric",
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
  const currentMonth = now.toLocaleString("en-US", { month: "long" });
  const currentMonthYear = now.toLocaleString("en-US", {
    month: "long",
    year: "numeric",
  });
  const sentEmailsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(
    `${currentMonthYear} Sent Emails`
  );

  // Loop for each row in the Email Reminders sheet
  for (const row of data) {
    const stateCode = row[0];
    const name = row[2];
    const emailDestination = row[3];
    const district = row[4];
    const exactLoginTime = row[5];
    const totalOpportunities = row[7];

    // Only email staff with no time in the login cell, AND who have opportunities,
    // AND whose district is known and not excluded
    if (
      !exactLoginTime &&
      totalOpportunities &&
      district &&
      !EXCLUDED_DISTRICTS.includes(district)
    ) {
      let toolName;
      if (stateCode === "US_MI") {
        toolName = "Recidiviz";
      } else if (stateCode === "US_IX") {
        toolName = "the P&P Assistant Tool";
      } else if (stateCode === "US_TN") {
        toolName = "the Compliant Reporting Recidiviz Tool";
      } else if (stateCode === "US_ME") {
        toolName = "the Recidiviz tool";
      } else if (stateCode === "US_ND") {
        toolName = "the Recidiviz early termination tool";
      }

      let body =
        `Hi ${name},<br><br>` +
        `We hope you're doing well! We noticed you haven’t logged into ${toolName} yet in ${currentMonth}, here’s what you might’ve missed:<br><br>` +
        `As of ${formattedDate} EST:<br>`;

      // Add total opportunities information if available
      if (stateCode === "US_MI") {
        body += `- There are ${totalOpportunities} eligible opportunities for clients under your supervision, such as early discharge or classification review.<br>`;
      } else if (stateCode === "US_TN") {
        body += `- There are ${totalOpportunities} eligible opportunities for clients under your supervision, such as compliant reporting or supervision level downgrade.<br>`;
      } else if (stateCode === "US_IX") {
        body += `- There are ${totalOpportunities} potential opportunities for clients under your supervision to receive a supervision level change, early discharge, or other milestone.<br>`;
      } else if (stateCode === "US_ME") {
        body += `- There are ${totalOpportunities} clients under your supervision eligible for early termination.<br>`;
      } else if (stateCode === "US_ND") {
        body += `- There are ${totalOpportunities} clients under your supervision eligible for early termination.<br>`;
      }

      body +=
        "<br>" +
        `<a href="${RECIDIVIZ_LINK}">${RECIDIVIZ_LINK_TEXT}</a><br><br>` +
        "Thank you for your dedication, and we look forward to seeing you back on Recidiviz soon!<br><br>" +
        "Best,<br>" +
        "The Recidiviz Team<br><br>" +
        `<i>Recidiviz is testing sending these reminder emails 1 week before the last business day each month. If you believe you’ve received this email in error or this email contains incorrect information, please email ${FEEDBACK_EMAIL} to let us know.</i>`;

      // Append data to "Sent Emails" sheet
      const formattedTimestamp = now.toLocaleString();
      sentEmailsSheet.appendRow([
        stateCode,
        name,
        emailDestination,
        district,
        formattedTimestamp,
      ]);

      // Send email with alias
      GmailApp.sendEmail(emailDestination, EMAIL_SUBJECT, "", {
        htmlBody: body,
        from: EMAIL_FROM_ALIAS,
        replyTo: FEEDBACK_EMAIL,
      });
    }
  }
}
