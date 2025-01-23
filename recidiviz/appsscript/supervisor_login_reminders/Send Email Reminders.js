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
/* Apps Script for sending email reminders to supervisors. */

const EXCLUDED_DISTRICTS = ["NOT_APPLICABLE", "EXTERNAL_UNKNOWN"];
const INCLUDED_STATES = ["US_IX", "US_MI", "US_TN"];

const EMAIL_FROM_ALIAS = "email-reports@recidiviz.org";
const FEEDBACK_EMAIL = "feedback@recidiviz.org";

const EMAIL_SUBJECT = "Recidiviz missed you this month!";
const RECIDIVIZ_LINK = "https://dashboard.recidiviz.org/";
const RECIDIVIZ_LINK_TEXT = "Login to Recidiviz";

// comma-separated list of state codes as strings
const statesForQuery = INCLUDED_STATES.map((s) => `"${s}"`).join();
const QUERY = `WITH supervisors AS (
-- TODO(#35758): Query a single aggregated metrics view here.
    SELECT DISTINCT
        supervisors.state_code, 
        supervisors.external_id AS supervisor_external_id, 
        CONCAT(INITCAP(JSON_VALUE(full_name, "$.given_names")), " ", INITCAP(JSON_VALUE(full_name, "$.surname"))) AS supervisor_name,
        email AS supervisor_email,
        supervision_district AS district,
    FROM \`recidiviz-123.outliers_views.supervision_officer_supervisors_materialized\` supervisors
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, supervisor_external_id
    ORDER BY district IS NULL, email IS NULL) = 1
)
, supervisor_latest_logins AS (
-- TODO(#35752): Remove this sub-query once the auth0 API is called directly.
    SELECT
        supervisors.state_code,
        supervisors.supervisor_external_id,
        supervisors.supervisor_name,
        supervisors.supervisor_email,
        supervisors.district,
        MAX(logins.timestamp) AS most_recent_login,
        CAST(DATE_TRUNC(MAX(logins.timestamp), MONTH) AS DATE) AS login_month,
    FROM \`recidiviz-123.auth0_prod_action_logs.success_login\` logins
    INNER JOIN supervisors
        ON LOWER(logins.email) = LOWER(supervisors.supervisor_email)
    WHERE CAST(DATE_TRUNC(logins.timestamp, MONTH) AS DATE) = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
    GROUP BY 1, 2, 3, 4, 5
),
latest_outliers AS (
  SELECT
        officers.state_code,
        supervisor_external_id,
        COUNT(DISTINCT officers.external_id) AS total_outliers,
    FROM \`recidiviz-123.outliers_views.supervision_officers_materialized\` officers,
    UNNEST(supervisor_external_ids) AS supervisor_external_id
    INNER JOIN \`recidiviz-123.outliers_views.supervision_officer_outlier_status_materialized\` outliers
      ON officers.state_code=outliers.state_code
      AND officers.external_id=outliers.officer_id
    WHERE supervisor_external_id IS NOT NULL
    AND status = 'FAR'
    AND end_date = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
    GROUP BY 1,2
),
latest_eligible_opportunities AS (
    SELECT
    eligible_population.state_code,
    staff.email AS supervisor_email,
    COUNT(DISTINCT CONCAT(eligible_population.person_id, task_type)) AS total_opportunities
  FROM \`recidiviz-123.analyst_data.workflows_person_impact_funnel_status_sessions_materialized\` eligible_population
  LEFT JOIN \`recidiviz-123.sessions.supervision_unit_supervisor_sessions_materialized\` supervisor_sessions
    ON eligible_population.person_id = supervisor_sessions.person_id
    AND eligible_population.state_code = supervisor_sessions.state_code
    AND CURRENT_DATE("US/Eastern") BETWEEN supervisor_sessions.start_date 
      AND COALESCE(DATE_SUB(supervisor_sessions.end_date_exclusive, INTERVAL 1 DAY), "9999-12-31")
  LEFT JOIN \`recidiviz-123.normalized_state.state_staff\` staff
    ON supervisor_sessions.unit_supervisor = staff.staff_id
  WHERE (is_eligible OR is_almost_eligible) AND NOT marked_ineligible
    AND CURRENT_DATE("US/Eastern") BETWEEN eligible_population.start_date 
      AND COALESCE(DATE_SUB(eligible_population.end_date, INTERVAL 1 DAY), "9999-12-31")
  GROUP BY 1, 2
)
SELECT
    supervisors.state_code,
    supervisors.supervisor_external_id,
    supervisors.supervisor_name,
    supervisors.supervisor_email,
    supervisors.district,
    supervisor_latest_logins.most_recent_login,
    latest_outliers.total_outliers,
    latest_eligible_opportunities.total_opportunities,
FROM supervisors
LEFT JOIN supervisor_latest_logins
  ON LOWER(supervisor_latest_logins.supervisor_email) = LOWER(supervisors.supervisor_email)
LEFT JOIN latest_eligible_opportunities
  ON LOWER(latest_eligible_opportunities.supervisor_email) = LOWER(supervisors.supervisor_email)
LEFT JOIN latest_outliers
  ON  supervisors.state_code = latest_outliers.state_code
  AND supervisors.supervisor_external_id = latest_outliers.supervisor_external_id
WHERE supervisors.state_code IN ( ${statesForQuery} )`;

function sendSupervisorEmailReminders() {
  const data = RecidivizHelpers.runQuery(QUERY);
  if (!data) {
    console.log("Failed to send emails: found no supervisors to email.");
    return;
  }

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
    `${currentMonthYear} Sent Emails to Supervisors`
  );

  for (const row of data) {
    const stateCode = row[0];
    const name = row[2];
    const emailDestination = row[3];
    const district = row[4];
    const exactLoginTime = row[5];
    const totalOutliers = row[6];
    const totalOpportunities = row[7];

    // Only email supervisors with no time in the login cell, AND who either have
    // outliers or opportunities, AND whose district is known and not excluded
    if (
      !exactLoginTime &&
      (totalOutliers || totalOpportunities) &&
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
      }

      let body =
        `Hi ${name},<br><br>` +
        `We hope you're doing well! We noticed you haven’t logged into ${toolName} yet in ${currentMonth}, here’s what you might’ve missed:<br><br>` +
        `As of ${formattedDate} EST:<br>`;

      // Add total outliers information if available
      if (totalOutliers) {
        if (stateCode === "US_MI") {
          body += `- ${totalOutliers} of your agents have been flagged as having high rates of absconder warrants or incarcerations rates.<br>`;
        } else if (stateCode === "US_IX") {
          body += `- Across your staff’s caseloads, there are ${totalOutliers} potential opportunities for clients to be reviewed for changes to their supervision level or early discharge.<br>`;
        }
      }

      // Add total opportunities information if available
      if (totalOpportunities) {
        let oppExamples;
        if (stateCode === "US_MI") {
          oppExamples = "early discharge or classification review";
        } else if (stateCode === "US_TN") {
          oppExamples = "compliant reporting or supervision level downgrade";
        } else if (stateCode === "US_IX") {
          oppExamples = "early discharge or transfer to limited supervision";
        }
        body += `- There are ${totalOpportunities} eligible opportunities for clients under your supervision, such as ${oppExamples}.<br>`;
      }

      body +=
        "<br>" +
        `<a href="${RECIDIVIZ_LINK}">${RECIDIVIZ_LINK_TEXT}</a><br><br>` +
        "Thank you for your dedication, and we look forward to seeing you back on Recidiviz soon!<br><br>" +
        "Best,<br>" +
        "The Recidiviz Team<br><br>" +
        `<i>If you believe you’ve received this email in error or this email contains incorrect information, please email ${FEEDBACK_EMAIL} to let us know.</i>`;

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
