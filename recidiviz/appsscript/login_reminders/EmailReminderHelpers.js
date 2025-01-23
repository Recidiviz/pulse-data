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
/* Apps Script helper functions for sending email reminders. */

/**
 * @param {string} stateCode the state of the person being emailed
 * @param {number} totalOpportunities the number of opportunities surfaced for them
 * @param {number} totalOutliers the number of outliers surfaced for them
 * @returns an object with state-specific text for this outbound email, with type
 * {
 *  toolName: string;
 *  timeZone: string;
 *  opportunitiesText: string;
 *  outliersText?: string;
 * }
 */
function stateSpecificText(stateCode, totalOpportunities, totalOutliers) {
  // Note: Many states are in multiple timezones. We use the zone with more people.
  switch (stateCode) {
    case "US_IX":
      return {
        toolName: "the P&P Assistant Tool",
        timeZone: "America/Boise",
        opportunitiesText: `There are ${totalOpportunities} potential opportunities for clients under your supervision to receive a supervision level change, early discharge, or other milestone.`,
        outliersText: `Across your staff’s caseloads, there are ${totalOutliers} potential opportunities for clients to be reviewed for changes to their supervision level or early discharge.`,
      };
    case "US_ME":
      return {
        toolName: "the Recidiviz tool",
        timeZone: "America/New_York",
        opportunitiesText: `There are ${totalOpportunities} clients under your supervision eligible for early termination.`,
      };
    case "US_MI":
      return {
        toolName: "Recidiviz",
        timeZone: "America/Detroit",
        opportunitiesText: `There are ${totalOpportunities} eligible opportunities for clients under your supervision, such as early discharge or classification review.`,
        outliersText: `${totalOutliers} of your agents have been flagged as having high rates of absconder warrants or incarcerations rates.`,
      };
    case "US_ND":
      return {
        toolName: "the Recidiviz early termination tool",
        timeZone: "America/Chicago",
        opportunitiesText: `There are ${totalOpportunities} clients under your supervision eligible for early termination.`,
      };
    case "US_TN":
      return {
        toolName: "the Compliant Reporting Recidiviz Tool",
        timeZone: "America/Chicago",
        opportunitiesText: `There are ${totalOpportunities} eligible opportunities for clients under your supervision, such as compliant reporting or supervision level downgrade.`,
      };
  }
}

/**
 * @returns the provided text formatted in a <li> tag if not empty, or the empty string
 */
function textToBulletPoint(text) {
  return text ? `<li>${text}</li>` : "";
}

/**
 * @returns the HTML body of the email to be sent to the person described in info
 */
function buildLoginReminderBody(info, settings) {
  const { stateCode, name, outliers, opportunities } = info;
  const { RECIDIVIZ_LINK, RECIDIVIZ_LINK_TEXT, FEEDBACK_EMAIL } = settings;

  const { toolName, timeZone, opportunitiesText, outliersText } =
    stateSpecificText(stateCode, opportunities, outliers);

  const now = new Date();
  const formattedDate = now.toLocaleString("en-US", {
    timeZone,
    timeZoneName: "short",
    hour12: true,
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
  const currentMonth = now.toLocaleString("en-US", {
    timeZone,
    month: "long",
  });

  return (
    `Hi ${name},<br><br>` +
    `We hope you're doing well! We noticed you haven’t logged into ${toolName} yet in ${currentMonth}, here’s what you might’ve missed:<br><br>` +
    `As of ${formattedDate}:<br>` +
    "<ul>" +
    textToBulletPoint(outliersText) +
    textToBulletPoint(opportunitiesText) +
    "</ul>" +
    `<a href="${RECIDIVIZ_LINK}">${RECIDIVIZ_LINK_TEXT}</a><br><br>` +
    "Thank you for your dedication, and we look forward to seeing you back on Recidiviz soon!<br><br>" +
    "Best,<br>" +
    "The Recidiviz Team<br><br>" +
    `<i>If you believe you’ve received this email in error or this email contains incorrect information, please email ${FEEDBACK_EMAIL} to let us know.</i>`
  );
}

/**
 * Send the email with body provided in `body` to the person whose data is provided,
 * and log that an email has been sent in the sentEmailsSheet.
 */
function sendLoginReminder(info, body, sentEmailsSheet, settings) {
  const { stateCode, name, emailAddress, district } = info;
  const { EMAIL_SUBJECT, EMAIL_FROM_ALIAS, FEEDBACK_EMAIL } = settings;

  // Add a record of this email to the sent emails spreadsheet
  const formattedTimestamp = new Date().toLocaleString("en-US", {
    timeZone: "America/New_York",
  });
  sentEmailsSheet.appendRow([
    stateCode,
    name,
    emailDestination,
    district,
    formattedTimestamp,
  ]);

  // Send the email from the appropriate alias
  GmailApp.sendEmail(emailAddress, EMAIL_SUBJECT, "", {
    htmlBody: body,
    from: EMAIL_FROM_ALIAS,
    replyTo: FEEDBACK_EMAIL,
  });
}

/**
 * @param {boolean} checkOutliers true when we should send an email if someone has
 *                                outliers but no opportunities
 * @returns true if we should send an email to the person described in `row`
 */
function shouldSendLoginReminder(info, checkOutliers, settings) {
  const { stateCode, district, lastLogin, outliers, opportunities } = info;
  const hasOutliersOrOpportunities = checkOutliers
    ? outliers || opportunities
    : opportunities;
  const { EXCLUDED_DISTRICTS } = settings;

  // Only email staff with no time in the login cell, AND who have outliers/opportunities,
  // AND whose district & state are known and not excluded
  return (
    !lastLogin &&
    hasOutliersOrOpportunities &&
    district &&
    !EXCLUDED_DISTRICTS.includes(district)
  );
}

/**
 * Send login reminder emails to all people surfaced by the provided query.
 * @param {boolean} isSupervisors true if emailing supervisors, false if linestaff
 * @param {string} query a BigQuery query collecting all people eligible for emails.
 *                       the expected shape of results is
 *                       [state code, external id, name, email, district,
 *                       last login time, # outliers, # opportunities]
 * @param {object} settings settings describing the email to be sent:
 *                          contains RECIDIVIZ_LINK, RECIDIVIZ_LINK_TEXT, FEEDBACK_EMAIL,
 *                          EMAIL_SUBJECT, EMAIL_FROM_ALIAS, EXCLUDED_DISTRICTS
 */
function sendAllLoginReminders(isSupervisors, query, settings) {
  const users = isSupervisors ? "Supervisors" : "Linestaff";

  const data = RecidivizHelpers.runQuery(query);
  if (!data) {
    console.log(`Failed to send emails: found no ${users} to email.`);
    return;
  }

  const currentMonthYear = new Date().toLocaleString("en-US", {
    timeZone: "America/New_York",
    month: "long",
    year: "numeric",
  });
  const sentEmailsSheet = SpreadsheetApp.getActiveSpreadsheet().insertSheet(
    `${currentMonthYear} Sent Emails to ${users}`
  );
  for (const row of data) {
    const emailInfo = {
      stateCode: row[0],
      name: row[2],
      emailAddress: row[3],
      district: row[4],
      lastLogin: row[5],
      outliers: row[6],
      opportunities: row[7],
    };
    if (shouldSendLoginReminder(emailInfo, isSupervisors, settings)) {
      const body = buildLoginReminderBody(emailInfo, settings);
      sendLoginReminder(emailInfo, body, sentEmailsSheet, settings);
    }
  }
}
