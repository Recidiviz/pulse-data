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
 * 1. query BigQuery for all facilities linestaff, supervision linestaff, or supervisors in the states we'd like to email
 * 2. query Auth0 to see which emails from step 1 haven't logged in this month
 * 3. for each user, assemble an email body with copy depending on the user's state,
 *    number of outstanding opportunities, etc. (from BQ)
 * 4. send emails using AppsScript's GmailApp connection
 */

// Options to define userType
const SUPERVISORS = "Supervisors";
const SUPERVISION_LINESTAFF = "Supervision Linestaff";
const FACILITIES_LINESTAFF = "Facilities Linestaff";
// If there is a new userType value, please add it to VALID_USER_TYPES
const VALID_USER_TYPES = [
  SUPERVISORS,
  SUPERVISION_LINESTAFF,
  FACILITIES_LINESTAFF,
];

// Global EMAIL_SETTINGS that applies to all types of email reminders
const EMAIL_SETTINGS = {
  EXCLUDED_DISTRICTS: ["NOT_APPLICABLE", "EXTERNAL_UNKNOWN"],

  EMAIL_FROM_ALIAS: "email-reports@recidiviz.org",
  FEEDBACK_EMAIL: "feedback@recidiviz.org",

  EMAIL_SUBJECT: "Recidiviz missed you this month!",
  RECIDIVIZ_LINK: "https://dashboard.recidiviz.org/",
  RECIDIVIZ_LINK_TEXT: "Login to Recidiviz",
};

/**
 * @param {string} stateCode the state of the person being emailed
 * @param {number} totalOpportunities the sum of almost eligible and eligible opportunities
 * @param {number} almostEligibleOpportunities the number of almost eligible opportunities
 * @param {number} totalOutliers the number of outliers surfaced for them
 * @returns an object with state-specific text for this outbound email, with type
 * {
 *  toolName: string;
 *  timeZone: string;
 *  facilitiesOpportunitiesText?: string;
 *  supervisionOportunitiesText?: string;
 *  outliersText?: string;
 * }
 */
function stateSpecificText(
  stateCode,
  totalOpportunities,
  almostEligibleOpportunities,
  totalOutliers
) {
  // Note: Many states are in multiple timezones. We use the zone with more people.
  switch (stateCode) {
    case "US_IX":
      return {
        toolName: "the P&P Assistant Tool",
        timeZone: "America/Boise",
        supervisionOpportunitiesText: `There are ${totalOpportunities} potential opportunities for clients under your supervision to receive a supervision level change, early discharge, or other milestone.`,
        outliersText: `${totalOutliers} of your officers have been flagged as having very high absconsion or incarceration rates.`,
      };
    case "US_ME":
      return {
        toolName: "the Recidiviz tool",
        timeZone: "America/New_York",
        supervisionOpportunitiesText: `There are ${totalOpportunities} clients under your supervision eligible for early termination.`,
      };
    case "US_MI":
      return {
        toolName: "Recidiviz",
        timeZone: "America/Detroit",
        supervisionOpportunitiesText: `There are ${totalOpportunities} eligible opportunities for clients under your supervision, such as early discharge or classification review.`,
        outliersText: `${totalOutliers} of your agents have been flagged as having very high absconder warrant or incarceration rates.`,
      };
    case "US_ND":
      return {
        toolName: "the Recidiviz early termination tool",
        timeZone: "America/Chicago",
        supervisionOpportunitiesText: `There are ${totalOpportunities} clients under your supervision eligible for early termination.`,
      };
    case "US_TN":
      return {
        toolName: "the Compliant Reporting Recidiviz Tool",
        timeZone: "America/Chicago",
        supervisionOpportunitiesText: `There are ${totalOpportunities} eligible opportunities for clients under your supervision, such as compliant reporting or supervision level downgrade.`,
      };
    case "US_AZ":
      return {
        toolName: "the Recidiviz tool",
        timeZone: "America/Phoenix",
        facilitiesOpportunitiesText: `There are ${almostEligibleOpportunities} inmates on your caseload who are almost eligible for transition release. Log into Recidiviz now to see what actions you can take to prepare for their release!`,
      };
  }
}

/**
 * @returns the HTML body of the email to be sent to the person described in `info`
 */
function buildLoginReminderBody(info, userType, settings) {
  const {
    stateCode,
    name,
    outliers,
    totalOpportunities,
    almostEligibleOpportunities,
  } = info;
  const { RECIDIVIZ_LINK, RECIDIVIZ_LINK_TEXT, FEEDBACK_EMAIL } = settings;
  const isSupervisors = userType === SUPERVISORS;
  const {
    toolName,
    timeZone,
    supervisionOpportunitiesText,
    facilitiesOpportunitiesText,
    outliersText,
  } = stateSpecificText(
    stateCode,
    totalOpportunities,
    almostEligibleOpportunities,
    outliers
  );

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
  const facilitiesOpportunitiesBulletPoint =
    facilitiesOpportunitiesText && totalOpportunities > 0
      ? `<li>${facilitiesOpportunitiesText}</li>`
      : "";
  const supervisionOpportunitiesBulletPoint =
    supervisionOpportunitiesText && totalOpportunities > 0
      ? `<li>${supervisionOpportunitiesText}</li>`
      : "";
  const opportunitiesBulletPoint =
    userType === FACILITIES_LINESTAFF
      ? facilitiesOpportunitiesBulletPoint
      : supervisionOpportunitiesBulletPoint;
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
  const {
    district,
    outliers,
    stateCode,
    totalOpportunities,
    almostEligibleOpportunities,
  } = info;
  const hasOutliersOrOpportunities = checkOutliers
    ? outliers || totalOpportunities
    : totalOpportunities;
  const { EXCLUDED_DISTRICTS } = settings;
  const hasAlmostEligibleOpportunities = almostEligibleOpportunities > 0;

  // For Arizona, we only want to email staff who have almost eligible opportunities
  if (stateCode === "US_AZ" && !hasAlmostEligibleOpportunities) {
    return false;
  }

  // For all other states, only email staff who have outliers/opportunities
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
 * @param {string} userType determines if the email is for FACILITIES_LINESTAFF,
 * SUPERVISION_LINESTAFF, or SUPERVISORS
 * @param {string} query a BigQuery query collecting all people eligible for emails.
 *                       the expected shape of results is
 *                       [state code, external id, name, email, district,
 *                       last login time, # outliers, # opportunities]
 * @param {object} settings settings describing the email to be sent:
 *                          contains RECIDIVIZ_LINK, RECIDIVIZ_LINK_TEXT, FEEDBACK_EMAIL,
 *                          EMAIL_SUBJECT, EMAIL_FROM_ALIAS, EXCLUDED_DISTRICTS
 */
function sendAllLoginReminders(userType, query, settings, stateCodes) {
  if (!VALID_USER_TYPES.includes(userType)) {
    throw new Error(
      `${userType} is not valid. userType should be one of the following values ${VALID_USER_TYPES.join(
        ", "
      )}.`
    );
  }
  console.log(`Getting list of all ${userType} from BigQuery...`);
  const data = RecidivizHelpers.runQuery(query);
  if (!data) {
    console.log(`Failed to send emails: found no ${userType} to email.`);
    return;
  }
  console.log(`Found ${data.length} ${userType}.`);

  // Make the sheet to log sent emails in if it doesn't already exist, and extract any
  // addresses we already sent to so that we don't re-email anyone
  const currentMonthYear = new Date().toLocaleString("en-US", {
    timeZone: "America/New_York",
    month: "long",
    year: "numeric",
  });
  const sheetName = `${currentMonthYear} Sent Emails to ${userType}`;
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

  const dataByEmail = Object.fromEntries(
    data.map((row) => [
      row[3].toLowerCase(), // email address, case normalized
      {
        stateCode: row[0],
        name: row[2],
        emailAddress: row[3].toLowerCase(),
        district: row[4],
        totalOpportunities: parseInt(row[5]),
        eligibleOpportunities: parseInt(row[6]),
        almostEligibleOpportunities: parseInt(row[7]),
        outliers: isSupervisors ? parseInt(row[8]) : 0,
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
        const body = buildLoginReminderBody(emailInfo, userType, settings);
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

/**
 * Adds a custom menu that allows users to send reminder emails from the spreadsheet
 */
function onOpen() {
  const sheet = SpreadsheetApp.getUi();
  sheet
    .createMenu("Send Emails")
    .addItem(
      "Send facilities line staff emails",
      "sendFacilitiesLinestaffEmailRemindersMenuItem"
    )
    .addItem(
      "Send supervision line staff emails",
      "sendSupervisionLinestaffEmailRemindersMenuItem"
    )
    .addItem("Send supervisor emails", "sendSupervisorEmailRemindersMenuItem")
    .addItem("Check login status", "checkLoginStatusMenuItem")
    .addToUi();
}

/**
 * Menu item for sending facilities line staff emails
 */
function sendFacilitiesLinestaffEmailRemindersMenuItem() {
  const sheet = SpreadsheetApp.getUi();
  const confirmation = getConfirmation(sheet, FACILITIES_LINESTAFF);

  if (confirmation) {
    sendFacilitiesLinestaffEmailReminders_();
    sheet.alert("Facilities Line staff emails sent successfully!");
  }
}

/**
 * Menu item for sending supervision line staff emails
 */
function sendSupervisionLinestaffEmailRemindersMenuItem() {
  const sheet = SpreadsheetApp.getUi();
  const confirmation = getConfirmation(sheet, SUPERVISION_LINESTAFF);

  if (confirmation) {
    sendSupervisionLinestaffEmailReminders_();
    sheet.alert("Supervision line staff emails sent successfully!");
  }
}

/**
 * Menu item for sending supervisor emails
 */
function sendSupervisorEmailRemindersMenuItem() {
  const sheet = SpreadsheetApp.getUi();
  const confirmation = getConfirmation(sheet, SUPERVISORS);

  if (confirmation) {
    sendSupervisorEmailReminders_();
    sheet.alert("Supervisor emails sent successfully!");
  }
}

/**
 * Menu item for checking login status
 */
function checkLoginStatusMenuItem() {
  const sheetUI = SpreadsheetApp.getUi();
  const sheets = SpreadsheetApp.getActiveSpreadsheet().getSheets().slice(0, 2);

  // Confirms with user if these are the two sheets they want to check login status for
  const response = sheetUI.alert(
    "Checking the login status for the leftmost two sheets.",
    `The sheets are ${sheets[0].getName()} and ${sheets[1].getName()}. \n Do you wish to proceed?`,
    sheetUI.ButtonSet.YES_NO
  );

  if (response === sheetUI.Button.YES) {
    // checkLoginStatus returns the login summaries
    const statements = checkLoginStatus();
    sheetUI.alert(
      "Checked login status successfully!\n" + statements.join(" \n")
    );
  } else {
    sheetUI.alert("Move the two sheets you want to check to the left");
  }
}

/**
 * Triggers a confirmation alert before sending emails
 * @param {Ui} sheet the linked email reminders spreadsheet
 * @param {string} userType indicates the user group we are emailing: FACILITIES_LINESTAFF,
 * SUPERVISION_LINESTAFF, or SUPERVISORS
 * @returns true if the user confirms, false otherwise
 */
function getConfirmation(sheet, userType) {
  const result = sheet.alert(
    "Please confirm",
    `This will send login reminder emails to ${userType}. 
    Are you sure you want to continue?`,
    sheet.ButtonSet.YES_NO
  );

  return result === sheet.Button.YES;
}
