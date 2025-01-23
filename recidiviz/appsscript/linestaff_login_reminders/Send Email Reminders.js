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
const INCLUDED_STATES = ["US_IX", "US_ME", "US_MI", "US_ND", "US_TN"];

const EMAIL_FROM_ALIAS = "email-reports@recidiviz.org";
const FEEDBACK_EMAIL = "feedback@recidiviz.org";

const EMAIL_SUBJECT = "Recidiviz missed you this month!";
const RECIDIVIZ_LINK = "https://dashboard.recidiviz.org/";
const RECIDIVIZ_LINK_TEXT = "Login to Recidiviz";

// comma-separated list of state codes as strings
const statesForQuery = INCLUDED_STATES.map((s) => `"${s}"`).join();
const QUERY = `WITH officers AS (
-- TODO(#35758): Query a single aggregated metrics view here.
    SELECT DISTINCT
        supervision_staff.state_code, 
        supervision_staff.officer_external_id, 
        CONCAT(INITCAP(JSON_VALUE(full_name, "$.given_names")), " ", INITCAP(JSON_VALUE(full_name, "$.surname"))) AS officer_name,
        email AS officer_email,
        COALESCE(attrs.supervision_district_name,attrs.supervision_district_name_inferred) AS district,
    FROM (
      -- A supervision officer in the Outliers product is anyone that has open session 
      -- in supervision_officer_sessions, in which they are someone's supervising officer 
      SELECT DISTINCT
          state_code,
          supervising_officer_external_id AS officer_external_id
      FROM \`recidiviz-123.sessions.supervision_officer_sessions_materialized\`
      WHERE
          -- Only include officers who have open officer sessions
          CURRENT_DATE("US/Pacific") BETWEEN start_date AND IFNULL(end_date, "9999-12-31")
    ) supervision_staff
    INNER JOIN (
        SELECT *
        FROM \`recidiviz-123.sessions.supervision_staff_attribute_sessions_materialized\`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, officer_id ORDER BY COALESCE(end_date_exclusive, "9999-01-01") DESC) = 1
    ) attrs
        ON attrs.state_code = supervision_staff.state_code AND attrs.officer_id = supervision_staff.officer_external_id 
    INNER JOIN \`recidiviz-123.normalized_state.state_staff\` staff 
        ON attrs.staff_id = staff.staff_id AND attrs.state_code = staff.state_code
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, officer_external_id
    ORDER BY district IS NULL, email IS NULL) = 1
)
, officer_latest_logins AS (
-- TODO(#35752): Remove this sub-query once the auth0 API is called directly.
    SELECT
        officers.state_code,
        officers.officer_external_id,
        officers.officer_name,
        officers.officer_email,
        officers.district,
        MAX(logins.timestamp) AS most_recent_login,
        CAST(DATE_TRUNC(MAX(logins.timestamp), MONTH) AS DATE) AS login_month,
    FROM \`recidiviz-123.auth0_prod_action_logs.success_login\` logins
    INNER JOIN officers
        ON LOWER(logins.email) = LOWER(officers.officer_email)
    WHERE CAST(DATE_TRUNC(logins.timestamp, MONTH) AS DATE) = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
    GROUP BY 1, 2, 3, 4, 5
),
latest_eligible_opportunities AS (
    SELECT
        surfaceable_population.state_code,
        staff.email AS officer_email,
        STRING_AGG(
            DISTINCT INITCAP(REPLACE(completion_event_type, "_", "  "))
            ORDER BY INITCAP(REPLACE(completion_event_type, "_", "  "))
        ) AS eligible_opportunity_types,
        COUNT(DISTINCT CONCAT(surfaceable_population.person_id, completion_event_type)) AS total_opportunities
    FROM
        \`recidiviz-123.analyst_data.workflows_record_archive_surfaceable_person_sessions_materialized\` surfaceable_population
    INNER JOIN
        \`recidiviz-123.reference_views.workflows_opportunity_configs_materialized\` config
    USING
        (state_code, opportunity_type)
    INNER JOIN
        \`recidiviz-123.analyst_data.workflows_live_completion_event_types_by_state_materialized\` launches
    USING
        (state_code, completion_event_type)
    LEFT JOIN
        \`recidiviz-123.analyst_data.all_task_type_marked_ineligible_spans_materialized\` ineligible_spans
    ON
        ineligible_spans.state_code = surfaceable_population.state_code
        AND ineligible_spans.person_id = surfaceable_population.person_id
        AND ineligible_spans.task_type = config.completion_event_type
        AND CURRENT_DATE("US/Pacific") BETWEEN ineligible_spans.start_date AND IFNULL(DATE_SUB(ineligible_spans.end_date_exclusive, INTERVAL 1 DAY), "9999-12-31")
    LEFT JOIN
        \`recidiviz-123.normalized_state.state_staff_external_id\` sei
        ON sei.state_code = surfaceable_population.state_code
        AND sei.external_id = surfaceable_population.caseload_id
    LEFT JOIN
        \`recidiviz-123.normalized_state.state_staff\` staff
    USING
        (staff_id)
    WHERE
        -- Restrict to open surfaceable sessions
        CURRENT_DATE("US/Pacific") BETWEEN surfaceable_population.start_date AND IFNULL(DATE_SUB(surfaceable_population.end_date_exclusive, INTERVAL 1 DAY), "9999-12-31")
        -- Do not count people currently marked ineligible
        AND ineligible_spans.person_id IS NULL
        -- Only count fully launched opportunities
        AND launches.is_fully_launched
    GROUP BY 1, 2
)
SELECT
    officers.state_code,
    officers.officer_external_id,
    officers.officer_name,
    officers.officer_email,
    officers.district,
    officer_latest_logins.most_recent_login,
    latest_eligible_opportunities.eligible_opportunity_types,
    latest_eligible_opportunities.total_opportunities,
FROM officers
LEFT JOIN officer_latest_logins
  ON LOWER(officer_latest_logins.officer_email) = LOWER(officers.officer_email)
LEFT JOIN latest_eligible_opportunities
  ON LOWER(latest_eligible_opportunities.officer_email) = LOWER(officers.officer_email)
WHERE officers.state_code IN ( ${statesForQuery} )`;

function sendLinestaffEmailReminders() {
  const data = RecidivizHelpers.runQuery(QUERY);
  if (!data) {
    console.log("Failed to send emails: found no line staff to email.");
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
    `${currentMonthYear} Sent Emails to Linestaff`
  );

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
