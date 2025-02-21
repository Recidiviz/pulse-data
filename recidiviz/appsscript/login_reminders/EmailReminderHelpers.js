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
 * Apps Script helper functions for sending email reminders.
 *
 * You should not run any of these functions from the Apps Script UI.
 *
 * The functions in this file handle all the steps of sending email reminders, namely:
 * 1. query BigQuery for all linestaff or supervisors in the states we'd like to email
 * 2. query Auth0 to see which emails from step 1 haven't logged in this month
 * 3. for each user, assemble an email body with copy depending on the user's state,
 *    number of outstanding opportunities, etc. (from BQ)
 * 4. send emails using AppsScript's GmailApp connection
 */

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
        outliersText: `${totalOutliers} of your officers have been flagged as having very high absconsion or incarceration rates.`,
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
        outliersText: `${totalOutliers} of your agents have been flagged as having very high absconder warrant or incarceration rates.`,
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
 * @returns the HTML body of the email to be sent to the person described in `info`
 */
function buildLoginReminderBody(info, isSupervisors, settings) {
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

  const opportunitiesBulletPoint =
    opportunitiesText && opportunities > 0
      ? `<li>${opportunitiesText}</li>`
      : "";
  const outliersBulletPoint =
    outliersText && isSupervisors && outliers > 0
      ? `<li>${outliersText}</li>`
      : "";

  return (
    `Hi ${name},<br><br>` +
    `We hope you're doing well! We noticed you haven’t logged into ${toolName} yet in ${currentMonth}, here’s what you might’ve missed:<br><br>` +
    `As of ${formattedDate}:<br>` +
    "<ul>" +
    outliersBulletPoint +
    opportunitiesBulletPoint +
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
  const formattedTimestamp = new Date().toISOString();
  sentEmailsSheet.appendRow([
    stateCode,
    name,
    emailAddress,
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
 * @returns true if we should send an email to the person described in `info`
 */
function shouldSendLoginReminder(info, checkOutliers, settings) {
  const { district, outliers, opportunities } = info;
  const hasOutliersOrOpportunities = checkOutliers
    ? outliers || opportunities
    : opportunities;
  const { EXCLUDED_DISTRICTS } = settings;

  // Only email staff who have outliers/opportunities
  // AND whose district & state are known and not excluded
  return (
    hasOutliersOrOpportunities &&
    district &&
    !EXCLUDED_DISTRICTS.includes(district)
  );
}

/**
 * Send login reminder emails to all people surfaced by the provided query.
 * Creates a new sheet within the connected sheet to track what emails were sent.
 * @param {boolean} isSupervisors true if emailing supervisors, false if linestaff
 * @param {string} query a BigQuery query collecting all people eligible for emails.
 *                       the expected shape of results is
 *                       [state code, external id, name, email, district,
 *                       last login time, # outliers, # opportunities]
 * @param {object} settings settings describing the email to be sent:
 *                          contains RECIDIVIZ_LINK, RECIDIVIZ_LINK_TEXT, FEEDBACK_EMAIL,
 *                          EMAIL_SUBJECT, EMAIL_FROM_ALIAS, EXCLUDED_DISTRICTS
 */
function sendAllLoginReminders(isSupervisors, query, settings, stateCodes) {
  const users = isSupervisors ? "Supervisors" : "Linestaff";

  console.log(`Getting list of all ${users} from BigQuery...`);
  const data = RecidivizHelpers.runQuery(query);
  if (!data) {
    console.log(`Failed to send emails: found no ${users} to email.`);
    return;
  }
  console.log(`Found ${data.length} ${users}.`);

  // Make the sheet to log sent emails in if it doesn't already exist, and extract any
  // addresses we already sent to so that we don't re-email anyone
  const currentMonthYear = new Date().toLocaleString("en-US", {
    timeZone: "America/New_York",
    month: "long",
    year: "numeric",
  });
  const sheetName = `${currentMonthYear} Sent Emails to ${users}`;
  const activeSheet = SpreadsheetApp.getActiveSpreadsheet();
  let sentEmailsSheet = activeSheet.getSheetByName(sheetName);
  if (!sentEmailsSheet) {
    sentEmailsSheet = activeSheet.insertSheet(sheetName);
    sentEmailsSheet.appendRow([
      "State Code",
      "Name",
      "Email",
      "District",
      "Email Sent At",
    ]);
  }
  const emailData = sentEmailsSheet.getDataRange();
  const emailsAlreadySent = emailData.getValues().map((row) => row[2]);

  // Convert the query results to allow for lookup by email address once we've
  // gotten login info from auth0
  // TODO(#35736): Update this if/when queries are changed
  const dataByEmail = Object.fromEntries(
    data.map((row) => [
      row[3].toLowerCase(), // email address, case normalized
      {
        stateCode: row[0],
        name: row[2],
        emailAddress: row[3].toLowerCase(),
        district: row[4],
        opportunities: parseInt(row[5]),
        outliers: isSupervisors ? parseInt(row[6]) : 0,
      },
    ])
  );
  const emails = Object.keys(dataByEmail);
  const hasOutliersTextConfigured = Object.fromEntries(
    stateCodes.map((stateCode) => [
      stateCode,
      !!stateSpecificText(stateCode, -1, -1).outliersText,
    ])
  );

  const cutoffDate = new Date(); // last day of the previous month, 11:59pm local time
  cutoffDate.setDate(0);
  cutoffDate.setHours(23, 59);

  console.log("Getting user login information from auth0...");
  const authToken = getAuth0Token();
  const userLoginInfo = getUserLoginInfo(emails, authToken);

  console.log("Sending emails...");

  for (const [email, lastLogin] of Object.entries(userLoginInfo)) {
    if (emailsAlreadySent.includes(email)) {
      console.log("Skipping person we already emailed:", email);
      continue;
    }
    if (lastLogin < cutoffDate) {
      const emailInfo = dataByEmail[email];
      const shouldCheckOutliers =
        isSupervisors && hasOutliersTextConfigured[emailInfo.stateCode];
      if (shouldSendLoginReminder(emailInfo, shouldCheckOutliers, settings)) {
        const body = buildLoginReminderBody(emailInfo, isSupervisors, settings);
        sendLoginReminder(emailInfo, body, sentEmailsSheet, settings);
      }
    }
  }

  console.log(
    `Done! Emails sent were written to the spreadsheet "${sheetName}".`
  );
}

/**
 * Fetch and return an Auth0 Management API access token using the client ID
 * and client secret specified in the Properties of this script (viewable through the
 * Apps Script UI)
 */
function getAuth0Token() {
  const properties = PropertiesService.getScriptProperties();
  const options = {
    method: "post",
    payload: {
      grant_type: "client_credentials",
      client_id: properties.getProperty("AUTH0_CLIENT_ID"),
      client_secret: properties.getProperty("AUTH0_CLIENT_SECRET"),
      audience: "https://recidiviz.auth0.com/api/v2/",
    },
  };
  const response = UrlFetchApp.fetch(
    "https://recidiviz.auth0.com/oauth/token",
    options
  );
  return JSON.parse(response.getContentText())["access_token"];
}

/**
 * @param {list} emails      a list of emails
 * @param {string} authToken an Auth0 Management API access token
 * @returns an object whose keys are lowercase emails and values are the most recent
 *          login for that email as a Date
 */
function getUserLoginInfo(emails, authToken) {
  // UrlFetchApp has a 2082-character URL length limit, so we send requests to
  // auth0 in small batches.
  // Note that auth0 will by default paginate the request if this is more than 50.
  const batchSize = 45;
  let result = {};
  for (let i = 0; i < emails.length / batchSize; i++) {
    const batch = emails.slice(i * batchSize, (i + 1) * batchSize);
    for (const info of getUserLoginInfoByBatch(batch, authToken)) {
      // Users can have multiple accounts under the same email in auth0,
      // we take the most recent login returned for this email
      const { email, lastLogin } = info;
      if (!(email in result) || lastLogin > result[email]) {
        result[email] = lastLogin;
      }
    }
  }
  return result;
}

/**
 * @param {list} batch       a list of emails that will fit into the UrlFetchApp length limit
 * @param {string} authToken an Auth0 Management API access token
 * @returns a list of shape [{email: str, lastLogin: Date}] for all provided emails,
 *          emails will be lowercase
 */
function getUserLoginInfoByBatch(batch, authToken) {
  // Format the emails for the query, e.g. email:("email1" OR "email2" OR "email3")
  const emailsForQuery = batch.map((s) => `"${s}"`).join(" OR ");
  const emailQuery = encodeURIComponent(`email:(${emailsForQuery})`);

  const url = `https://recidiviz.auth0.com/api/v2/users?search_engine=v3&fields=email,last_login&q=${emailQuery}`;
  const options = {
    headers: {
      authorization: `Bearer ${authToken}`,
    },
  };
  // UrlFetchApp apparently throws an error if the response code is not 200,
  // so we don't handle errors here ourselves
  const response = UrlFetchApp.fetch(url, options);

  return JSON.parse(response.getContentText()).map((o) => ({
    email: o.email.toLowerCase(),
    lastLogin: new Date(o.last_login),
  }));
}
