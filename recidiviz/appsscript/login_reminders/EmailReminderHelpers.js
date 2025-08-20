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

  RECIDIVIZ_LINK: "https://dashboard.recidiviz.org/",
  RECIDIVIZ_LINK_TEXT: "Login to Recidiviz",
};

/**
 * @param {string} stateCode the state of the person being emailed
 * @returns an object that maps task completion events with the state-specific
 * human-readable opportunity name, with type
 * {
 *  [event: string]: string
 * }
 */
function stateSpecificOpportunities(stateCode) {
  switch (stateCode) {
    case "US_IX":
      return {
        EARLY_DISCHARGE: "Earned Discharge",
        TRANSFER_TO_LIMITED_SUPERVISION: "Limited Supervision Unit",
        FULL_TERM_DISCHARGE: "Release from Supervision",
        SUPERVISION_LEVEL_DOWNGRADE: "Supervision Level Mismatch",
      };
    case "US_ME":
      return {
        EARLY_DISCHARGE: "Early Termination",
      };
    case "US_MI":
      return {
        SUPERVISION_LEVEL_DOWNGRADE_AFTER_INITIAL_CLASSIFICATION_REVIEW_DATE:
          "Classification Review",
        EARLY_DISCHARGE: "Early Discharge",
        TRANSFER_TO_LIMITED_SUPERVISION: "Minimum Telephone Reporting",
        FULL_TERM_DISCHARGE: "Overdue for Discharge",
        SUPERVISION_LEVEL_DOWNGRADE_BEFORE_INITIAL_CLASSIFICATION_REVIEW_DATE:
          "Supervision Level Mismatch",
      };
    case "US_ND":
      return {
        EARLY_DISCHARGE: "Early Termination",
      };
    case "US_NE":
      return {
        OVERRIDE_TO_CONDITIONAL_LOW_RISK_SUPERVISION:
          "Override to Conditional Low Risk",
        OVERRIDE_TO_LOW_SUPERVISION: "Override to Low",
      };
    case "US_PA":
      return {
        TRANSFER_TO_ADMINISTRATIVE_SUPERVISION: "Admin Supervision",
        TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION:
          "Special Circumstances Supervision",
      };
    case "US_TN":
      return {
        TRANSFER_TO_LIMITED_SUPERVISION: "Compliant Reporting",
        FULL_TERM_DISCHARGE: "Expiration",
        SUPERVISION_LEVEL_DOWNGRADE: "Supervision Level Downgrade",
        TRANSFER_TO_NO_CONTACT_PAROLE: "Suspension of Direct Supervision",
      };
    case "US_UT":
      return {
        EARLY_DISCHARGE: "Early Termination",
      };
  }
}

/**
 * Parses the clients by opportunity data into a simpler format because BQ
 * returns it in a deeply nested/hard to work with format. Also filters out
 * opportunities without any eligible clients.
 * @param {string} stateCode the state of the person being emailed
 * @param {array} structArray an array of structs representing the number of clients
 * by opportunity
 * @returns an object with state-specific opportunity names and number of clients, with type
 * {
 *  opportunityName: string;
 *  numClients: number;
 * }
 */
function parseClientsByOpportunity(stateCode, structArray) {
  const opportunityTypes = stateSpecificOpportunities(stateCode);

  return structArray
    .map((struct) => {
      const values = struct.v.f;

      const typeKey = values[0].v;
      const label = opportunityTypes[typeKey] || typeKey;
      const value = parseInt(values[1].v);

      return {
        opportunityName: label,
        numClients: value,
      };
    })
    .filter((item) => item.numClients !== 0);
}

/**
 * Logic to determine whether to use plural or singular verb/noun.
 * @param {number} count the number of nouns
 * @param {string[]} additionalVerbs verbs to conjugate other than "is" and "has"
 * @param {string} noun the name of the noun to pluralize
 * @returns an object with type
 * {
 *  is: string;
 *  has: string;
 *  [verbName]: string; // for each additional verb
 *  pluralNoun: string;
 * }
 */
function pluralize(count, additionalVerbs = [], noun = "client") {
  let pluralNoun;
  if (noun === "opportunity") {
    pluralNoun = count === 1 ? "opportunity" : "opportunities";
  } else {
    pluralNoun = count === 1 ? noun : `${noun}s`;
  }

  const result = {
    is: count === 1 ? "is" : "are",
    has: count === 1 ? "has" : "have",
    pluralNoun,
  };

  // Add additional verbs as separate keys
  additionalVerbs.forEach((verb) => {
    if (verb === "is" || verb === "has") {
      return;
    } else {
      result[verb] = count === 1 ? `${verb}s` : verb;
    }
  });

  return result;
}

function generateClientText(clientCounts, opportunityName) {
  if (clientCounts.urgentCount) {
    const urgent = pluralize(clientCounts.urgentCount);
    return `There ${urgent.is} ${clientCounts.urgentCount} ${urgent.pluralNoun} under your supervision that ${urgent.has} been eligible for ${opportunityName} for over 30 days who ${urgent.has}n’t yet been reviewed in the tool.`;
  }

  const eligible = pluralize(clientCounts.eligibleCount);
  const almost = pluralize(clientCounts.almostEligibleCount);

  if (clientCounts.eligibleCount && clientCounts.almostEligibleCount) {
    return `There ${eligible.is} ${clientCounts.eligibleCount} ${eligible.pluralNoun} under your supervision eligible for ${opportunityName}. There ${almost.is} ${clientCounts.almostEligibleCount} additional ${almost.pluralNoun} who ${almost.is} almost eligible for ${opportunityName}.`;
  } else if (clientCounts.eligibleCount && !clientCounts.almostEligibleCount) {
    return `There ${eligible.is} ${clientCounts.eligibleCount} ${eligible.pluralNoun} under your supervision eligible for ${opportunityName}.`;
  } else if (!clientCounts.eligibleCount && clientCounts.almostEligibleCount) {
    return `There ${almost.is} ${clientCounts.almostEligibleCount} ${almost.pluralNoun} under your supervision who ${almost.is} almost eligible for ${opportunityName}.`;
  } else {
    return "";
  }
}

/**
 * @param {array} eligibleClients the opportunities with eligible clients
 * @param {array} almostEligibleClients the opportunities with almost eligible clients
 * @param {array} urgentClients the opportunities with urgent clients
 * @returns an array of strings with opportunity-specific text
 */
function generateOpportunitySpecificText(
  eligibleClients,
  almostEligibleClients,
  urgentClients
) {
  const opportunityNames = new Set([
    ...eligibleClients.map((c) => c.opportunityName),
    ...almostEligibleClients.map((c) => c.opportunityName),
  ]);

  const opportunities = Array.from(opportunityNames).map((opportunityName) => {
    const clientCounts = {
      urgentCount:
        urgentClients?.find((o) => o.opportunityName === opportunityName)
          ?.numClients ?? 0,
      eligibleCount:
        eligibleClients?.find((o) => o.opportunityName === opportunityName)
          ?.numClients ?? 0,
      almostEligibleCount:
        almostEligibleClients?.find(
          (o) => o.opportunityName === opportunityName
        )?.numClients ?? 0,
    };

    return {
      text: generateClientText(clientCounts, opportunityName),
      clientCounts,
    };
  });

  // Sort so urgent opportunities appear first, then within non-urgent sort by eligible first, then almost eligible
  opportunities.sort((a, b) => {
    if (a.clientCounts.urgentCount && !b.clientCounts.urgentCount) return -1;
    if (!a.clientCounts.urgentCount && b.clientCounts.urgentCount) return 1;

    if (a.clientCounts.urgentCount && b.clientCounts.urgentCount)
      return b.clientCounts.urgentCount - a.clientCounts.urgentCount;

    if (a.clientCounts.eligibleCount !== b.clientCounts.eligibleCount)
      return b.clientCounts.eligibleCount - a.clientCounts.eligibleCount;

    return (
      b.clientCounts.almostEligibleCount - a.clientCounts.almostEligibleCount
    );
  });

  return opportunities.map((opp) => opp.text);
}

/**
 * @param {string} stateCode the state of the person being emailed
 * @param {number} totalOpportunities the sum of almost eligible and eligible opportunities
 * @param {number} almostEligibleOpportunities the number of almost eligible opportunities
 * @param {number} totalOutliers the number of outliers surfaced for them
 * @returns an object with state-specific text for this outbound email, with type
 * {
 *  supervisionToolName: string;
 *  facilitiesToolName: string;
 *  timeZone: string;
 *  facilitiesOpportunitiesText?: string;
 *  supervisionOportunitiesText?: string;
 *  outliersText?: string;
 *  supervisionOpportunitySpecificText?: string;
 * }
 */
function stateSpecificText(
  stateCode,
  totalOpportunities,
  almostEligibleOpportunities,
  totalOutliers,
  eligibleClientsByOpportunity,
  almostEligibleClientsByOpportunity,
  urgentClientsByOpportunity
) {
  const supervisionOpportunitySpecificText = generateOpportunitySpecificText(
    eligibleClientsByOpportunity,
    almostEligibleClientsByOpportunity,
    urgentClientsByOpportunity
  );

  const clients = pluralize(totalOpportunities, [], "client");
  const opps = pluralize(totalOpportunities, [], "opportunity");
  const officers = pluralize(totalOutliers, [], "officer");
  const inmates = pluralize(almostEligibleOpportunities, [], "inmate"); // Special case for Arizona
  const residents = pluralize(totalOpportunities, [], "resident");

  // Note: Many states are in multiple timezones. We use the zone with more people.
  switch (stateCode) {
    case "US_IX":
      return {
        supervisionToolName: "the P&P Assistant Tool",
        timeZone: "America/Boise",
        supervisionOpportunitiesText: `There ${opps.is} ${totalOpportunities} potential ${opps.pluralNoun} for clients under your supervision to receive a supervision level change, early discharge, or other milestone.`,
        supervisionOpportunitySpecificText,
        supervisionSupervisorText: `There ${opps.is} ${totalOpportunities} potential ${opps.pluralNoun} for clients under your officers' supervision to receive a supervision level change, early discharge, or other milestone.`,
        outliersText: `${totalOutliers} of your officers ${officers.has} been flagged as having very high absconsion or incarceration rates.`,
      };
    case "US_ME":
      return {
        supervisionToolName: "the Recidiviz tool",
        timeZone: "America/New_York",
        supervisionOpportunitiesText: `There ${clients.is} ${totalOpportunities} ${clients.pluralNoun} under your supervision eligible for early termination.`,
        supervisionOpportunitySpecificText,
      };
    case "US_MI":
      return {
        supervisionToolName: "Recidiviz",
        timeZone: "America/Detroit",
        supervisionOpportunitiesText: `There ${opps.is} ${totalOpportunities} eligible ${opps.pluralNoun} for clients under your supervision, such as early discharge or classification review.`,
        supervisionSupervisorText: `There ${opps.is} ${totalOpportunities} eligible ${opps.pluralNoun} for clients under your agents' supervision, such as early discharge or classification review.`,
        supervisionOpportunitySpecificText,
        outliersText: `${totalOutliers} of your agents ${officers.has} been flagged as having very high absconder warrant or incarceration rates.`,
      };
    case "US_ND":
      return {
        supervisionToolName: "the Recidiviz early termination tool",
        timeZone: "America/Chicago",
        supervisionOpportunitiesText: `There ${clients.is} ${totalOpportunities} ${clients.pluralNoun} under your supervision eligible for early termination.`,
        supervisionOpportunitySpecificText,
      };
    case "US_NE":
      return {
        supervisionToolName: "the Recidiviz Supervision Assistant",
        timeZone: "America/Chicago",
        supervisionOpportunitySpecificText,
      };
    case "US_PA":
      return {
        supervisionToolName: "the Recidiviz Supervision Assistant",
        timeZone: "America/New_York",
        supervisionOpportunitiesText: `There ${clients.is} ${totalOpportunities} ${clients.pluralNoun} under your supervision who may be eligible for Admin Supervision or Special Circumstances Supervision.`,
        supervisionSupervisorText: `There ${clients.is} ${totalOpportunities} ${clients.pluralNoun} under your agents' supervision who may be eligible for Admin Supervision or Special Circumstances Supervision.`,
        supervisionOpportunitySpecificText,
      };
    case "US_TN":
      return {
        supervisionToolName: "the Compliant Reporting Recidiviz Tool",
        facilitiesToolName: "the Recidiviz tool",
        timeZone: "America/Chicago",
        facilitiesOpportunitiesText: `There ${opps.is} ${totalOpportunities} ${residents.pluralNoun} on your caseload who may be eligible for opportunities such as annual reclassification, custody level downgrade, or initial classification. Log into Recidiviz now to see what actions you can take to prepare for their release!`,
        supervisionOpportunitiesText: `There ${opps.is} ${totalOpportunities} eligible ${opps.pluralNoun} for clients under your supervision, such as compliant reporting or supervision level downgrade.`,
        supervisionSupervisorText: `There ${opps.is} ${totalOpportunities} eligible ${opps.pluralNoun} for clients under your officers' supervision, such as compliant reporting or supervision level downgrade.`,
        supervisionOpportunitySpecificText,
      };
    case "US_AZ":
      return {
        facilitiesToolName: "the Recidiviz tool",
        timeZone: "America/Phoenix",
        facilitiesOpportunitiesText: `There ${inmates.is} ${almostEligibleOpportunities} ${inmates.pluralNoun} on your caseload who ${inmates.is} almost eligible for transition release. Log into Recidiviz now to see what actions you can take to prepare for their release!`,
      };
    case "US_UT":
      return {
        supervisionToolName: "the Recidiviz Supervision Assistant",
        timeZone: "America/Denver",
        supervisionOpportunitySpecificText,
      };
  }
}

/**
 * Generates the introduction text for the reminder email, which is different for supervision line
 * staff depending on whether they have any urgent clients and whether they've logged in during the current month
 * @param {string} toolName the state-specific tool name
 * @param {string} currentMonth the full name of the current month
 * @param {array} urgentClients the opportunities with urgent clients
 * @param {boolean} loggedIn whether the user has logged in during the current month
 * @returns a string with the appropriate intro text
 */
function generateIntroText(toolName, currentMonth, urgentClients, loggedIn) {
  if (urgentClients.length && loggedIn) {
    return `We hope you're doing well! We noticed some of your clients in ${toolName} have been eligible for over 30 days whom you haven’t reviewed yet. Clients can be resolved by either marking them as submitted or ineligible for the opportunity. Here’s what you might’ve missed:<br><br>`;
  }

  if (urgentClients.length) {
    return `We hope you're doing well! We noticed you haven’t logged into ${toolName} yet in ${currentMonth}. Some of your clients in the tool have been eligible for over 30 days that you haven’t yet resolved by either marking them as submitted or ineligible for the opportunity.<br><br>`;
  }

  return `We hope you're doing well! We noticed you haven’t logged into ${toolName} yet in ${currentMonth}, here’s what you might’ve missed:<br><br>`;
}

/**
 * @param {date} lastLoginDate the user's most recent login date from auth0
 * @returns a boolean indicating whether the most recent login date is during the current month
 */
function loggedInThisMonth(lastLoginDate) {
  const cutoffDate = new Date(); // last day of the previous month, 11:59pm local time
  cutoffDate.setDate(0);
  cutoffDate.setHours(23, 59);

  return lastLoginDate >= cutoffDate;
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
    eligibleOpportunities,
    almostEligibleOpportunities,
    eligibleClientsByOpportunity,
    almostEligibleClientsByOpportunity,
    urgentClientsByOpportunity,
    lastLogin,
  } = info;
  const { RECIDIVIZ_LINK, RECIDIVIZ_LINK_TEXT, FEEDBACK_EMAIL } = settings;
  const isSupervisors = userType === SUPERVISORS;
  const {
    supervisionToolName,
    facilitiesToolName,
    timeZone,
    supervisionOpportunitiesText,
    supervisionSupervisorText,
    facilitiesOpportunitiesText,
    outliersText,
    supervisionOpportunitySpecificText,
  } = stateSpecificText(
    stateCode,
    totalOpportunities,
    almostEligibleOpportunities,
    outliers,
    eligibleClientsByOpportunity,
    almostEligibleClientsByOpportunity,
    urgentClientsByOpportunity
  );

  const loggedIn = loggedInThisMonth(lastLogin);

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

  let introText = "";
  let bulletPoints = "";
  let additionalContent = "";

  if (userType === SUPERVISION_LINESTAFF) {
    introText = generateIntroText(
      supervisionToolName,
      currentMonth,
      urgentClientsByOpportunity,
      loggedIn
    );
    if (supervisionOpportunitySpecificText && totalOpportunities > 0) {
      bulletPoints = supervisionOpportunitySpecificText
        .map((text) => `<li>${text}</li>`)
        .join("");

      const eligible = pluralize(eligibleOpportunities);
      if (urgentClientsByOpportunity.length) {
        const urgentOpportunities = urgentClientsByOpportunity.reduce(
          (total, opp) => (total += opp.numClients),
          0
        );
        const urgent = pluralize(urgentOpportunities);
        additionalContent = `There ${eligible.is} ${eligibleOpportunities} total ${eligible.pluralNoun} eligible for opportunities, and ${urgentOpportunities} ${urgent.has} been unviewed for 30+ days.<br><br>`;
      } else {
        // We only need a summary if there was more than one opportunity type
        if (supervisionOpportunitySpecificText.length > 1) {
          const almost = pluralize(almostEligibleOpportunities);
          additionalContent = `There ${eligible.is} ${eligibleOpportunities} total ${eligible.pluralNoun} eligible for opportunities and ${almostEligibleOpportunities} total ${almost.is} almost eligible for opportunities.<br><br>`;
        }
      }
    }
  } else if (userType === FACILITIES_LINESTAFF) {
    introText = generateIntroText(
      facilitiesToolName,
      currentMonth,
      urgentClientsByOpportunity,
      loggedIn
    );
    if (facilitiesOpportunitiesText && totalOpportunities > 0) {
      bulletPoints = `<li>${facilitiesOpportunitiesText}</li>`;
    }
  } else if (isSupervisors) {
    introText = generateIntroText(
      supervisionToolName,
      currentMonth,
      urgentClientsByOpportunity,
      loggedIn
    );
    const outliersBulletPoint =
      outliersText && outliers > 0 ? `<li>${outliersText}</li>` : "";
    const supervisionBulletPoint =
      supervisionSupervisorText && totalOpportunities > 0
        ? `<li>${supervisionSupervisorText}</li>`
        : "";
    bulletPoints = outliersBulletPoint + supervisionBulletPoint;
  }

  return (
    `Hi ${name},<br><br>` +
    introText +
    `As of ${formattedDate}:<br>` +
    "<ul>" +
    bulletPoints +
    "</ul>" +
    additionalContent +
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
  const { stateCode, name, emailAddress, district, lastLogin } = info;
  const { EMAIL_FROM_ALIAS, FEEDBACK_EMAIL, IS_TESTING } = settings;
  const loggedIn = loggedInThisMonth(lastLogin);
  const emailSubjectPrefix = IS_TESTING ? `[TESTING ${stateCode}] ` : "";
  const emailSubject = loggedIn
    ? `${emailSubjectPrefix}Your month on Recidiviz!`
    : `${emailSubjectPrefix}Recidiviz missed you this month!`;

  // Add a record of this email to the sent emails spreadsheet
  const formattedTimestamp = new Date().toISOString();
  const emailRecord = [
    stateCode,
    name,
    emailAddress,
    district,
    formattedTimestamp,
  ];
  if (IS_TESTING) {
    emailRecord.push(...[emailSubject, body]);
  }
  sentEmailsSheet.appendRow(emailRecord);

  // Only send a real email if we are not testing or we're sending to a recidiviz email address.
  const isSendingToRecidivizEmail = emailAddress
    .toLowerCase()
    .includes("recidiviz.org");
  if (!IS_TESTING || isSendingToRecidivizEmail) {
    // Send the email from the appropriate alias
    GmailApp.sendEmail(emailAddress, emailSubject, "", {
      htmlBody: body,
      from: EMAIL_FROM_ALIAS,
      replyTo: FEEDBACK_EMAIL,
    });
  }
}

/**
 * @param {boolean} checkOutliers true when we should send an email if someone has
 *                                outliers but no opportunities
 * @returns true if we should send an email to the person described in `info`
 */
function shouldSendLoginReminder(info, checkOutliers, settings, userType) {
  const {
    district,
    outliers,
    stateCode,
    totalOpportunities,
    almostEligibleOpportunities,
    urgentClientsByOpportunity,
    lastLogin,
  } = info;
  const loggedIn = loggedInThisMonth(lastLogin);

  // We don't want to email non-supervision line staff who have logged in
  if (userType !== SUPERVISION_LINESTAFF && loggedIn) {
    return false;
  }

  // For now we also don't want to email supervision line staff who have logged in and don't
  // have any urgent clients
  if (
    userType === SUPERVISION_LINESTAFF &&
    loggedIn &&
    !urgentClientsByOpportunity.length
  ) {
    return false;
  }

  // For Arizona, we only want to email staff who have almost eligible opportunities
  const hasAlmostEligibleOpportunities = almostEligibleOpportunities > 0;
  if (stateCode === "US_AZ" && !hasAlmostEligibleOpportunities) {
    return false;
  }

  // If the staff's district is known and excluded, don't email them
  const { EXCLUDED_DISTRICTS } = settings;
  if (district && EXCLUDED_DISTRICTS.includes(district)) {
    return false;
  }

  // If we have text to display for outliers, email staff who have available outliers or opportunities
  if (checkOutliers) {
    return Boolean(outliers || totalOpportunities);
  }

  // Otherwise, only email staff who have any available opportunities, urgent or not
  return Boolean(totalOpportunities);
}

/**
 * This function is the main entry point to this script and is called externally to
 * send login reminder emails to all people surfaced by the provided query.
 * Creates a new sheet within the connected sheet to track what emails were sent.
 *
 * @param {string} userType determines if the email is for FACILITIES_LINESTAFF,
 * SUPERVISION_LINESTAFF, or SUPERVISORS
 * @param {string} query a BigQuery query collecting all people eligible for emails.
 *                       the expected shape of results is
 *                       [state code, external id, name, email, district,
 *                       last login time, # outliers, # opportunities]
 * @param {object} settings settings describing the email to be sent:
 *                          contains RECIDIVIZ_LINK, RECIDIVIZ_LINK_TEXT, FEEDBACK_EMAIL,
 *                          EMAIL_FROM_ALIAS, EXCLUDED_DISTRICTS
 *                          and has IS_TESTING set to true if this is a test environment
 * @param {string[]} stateCodes the list of state codes we'll attempt to email
 */
function sendAllLoginReminders(userType, query, settings, stateCodes) {
  if (!VALID_USER_TYPES.includes(userType)) {
    throw new Error(
      `${userType} is not valid. userType should be one of the following values ${VALID_USER_TYPES.join(
        ", "
      )}.`
    );
  }
  const { IS_TESTING } = settings;

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
  const testingPrefix = IS_TESTING ? `[TESTING] ` : "";
  const sheetName = `${testingPrefix}${currentMonthYear} Sent Emails to ${userType}`;
  const activeSheet = SpreadsheetApp.getActiveSpreadsheet();
  let sentEmailsSheet = activeSheet.getSheetByName(sheetName);
  if (!sentEmailsSheet) {
    sentEmailsSheet = activeSheet.insertSheet(sheetName);
    const headerRow = [
      "State Code",
      "Name",
      "Email",
      "District",
      "Email Sent At",
    ];
    if (IS_TESTING) {
      headerRow.push(...["Email Subject", "Email Body"]);
    }
    sentEmailsSheet.appendRow(headerRow);
  }
  const emailData = sentEmailsSheet.getDataRange();
  const emailsAlreadySent = emailData.getValues().map((row) => row[2]);

  // Convert the query results to allow for lookup by email address once we've
  // gotten login info from auth0
  const isSupervisionLinestaff = userType === SUPERVISION_LINESTAFF;
  const isSupervisors = userType === SUPERVISORS;
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
        eligibleClientsByOpportunity: isSupervisionLinestaff
          ? parseClientsByOpportunity(row[0], row[8])
          : [],
        almostEligibleClientsByOpportunity: isSupervisionLinestaff
          ? parseClientsByOpportunity(row[0], row[9])
          : [],
        urgentClientsByOpportunity: isSupervisionLinestaff
          ? parseClientsByOpportunity(row[0], row[10])
          : [],
      },
    ])
  );

  const emails = Object.keys(dataByEmail);
  const hasOutliersTextConfigured = Object.fromEntries(
    stateCodes.map((stateCode) => [
      stateCode,
      !!stateSpecificText(stateCode, -1, -1, -1, [], [], []).outliersText,
    ])
  );

  console.log("Getting user login information from auth0...");
  const authToken = getAuth0Token();
  const userLoginInfo = getUserLoginInfo(emails, authToken);

  console.log("Sending emails...");

  let emailsSentByState = Object.fromEntries(
    stateCodes.map((stateCode) => [stateCode, 0])
  );

  for (const [email, lastLogin] of Object.entries(userLoginInfo)) {
    if (!IS_TESTING && emailsAlreadySent.includes(email)) {
      console.log("Skipping person we already emailed:", email);
      continue;
    }

    const emailInfo = {
      ...dataByEmail[email],
      lastLogin,
    };

    if (
      IS_TESTING &&
      emailsSentByState[emailInfo.stateCode] >= TEST_EMAIL_LIMIT_PER_STATE
    )
      continue;

    const shouldCheckOutliers =
      isSupervisors && hasOutliersTextConfigured[emailInfo.stateCode];
    if (
      shouldSendLoginReminder(
        emailInfo,
        shouldCheckOutliers,
        settings,
        userType
      )
    ) {
      const body = buildLoginReminderBody(emailInfo, userType, settings);
      sendLoginReminder(emailInfo, body, sentEmailsSheet, settings);
      emailsSentByState[emailInfo.stateCode]++;
    }
  }

  console.log(
    `Done! Emails sent were written to the spreadsheet "${sheetName}".`
  );
}

/************************************************************************************
 * Functions related to getting user login info from auth0
 ************************************************************************************/

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

/************************************************************************************
 * Functions for creating user-friendly menu items within the email-sending spreadsheet
 ************************************************************************************/

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
    sheet.alert(
      "Facilities Line staff emails sent successfully! Please hide the previous month's sheet."
    );
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
    sheet.alert(
      "Supervision line staff emails sent successfully! Please hide the previous month's sheet."
    );
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
    sheet.alert(
      "Supervisor emails sent successfully! Please hide the previous month's sheet."
    );
  }
}

/**
 * Menu item for checking login status
 */
function checkLoginStatusMenuItem() {
  const sheetUI = SpreadsheetApp.getUi();
  const sheets = SpreadsheetApp.getActiveSpreadsheet()
    .getSheets()
    .slice(0, NUM_SHEETS_TO_CHECK_LOGIN_STATUS);

  // Confirms with user if these are the two sheets they want to check login status for
  const response = sheetUI.alert(
    `Checking the login status for the leftmost ${NUM_SHEETS_TO_CHECK_LOGIN_STATUS} sheets.`,
    `The sheets are: ${sheets
      .map((s) => s.getName())
      .join()}. \n Do you wish to proceed?`,
    sheetUI.ButtonSet.YES_NO
  );

  if (response === sheetUI.Button.YES) {
    // checkLoginStatus returns the login summaries
    const statements = checkLoginStatus();
    sheetUI.alert(
      "Checked login status successfully!\n" + statements.join(" \n")
    );
  } else {
    sheetUI.alert(
      `Move the ${NUM_SHEETS_TO_CHECK_LOGIN_STATUS} sheets you want to check to the left`
    );
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
