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

const SUPERVISOR_SETTINGS = {
  EXCLUDED_DISTRICTS: ["NOT_APPLICABLE", "EXTERNAL_UNKNOWN"],

  EMAIL_FROM_ALIAS: "email-reports@recidiviz.org",
  FEEDBACK_EMAIL: "feedback@recidiviz.org",

  EMAIL_SUBJECT: "Recidiviz missed you this month!",
  RECIDIVIZ_LINK: "https://dashboard.recidiviz.org/",
  RECIDIVIZ_LINK_TEXT: "Login to Recidiviz",
};

const SUPERVISOR_INCLUDED_STATES = ["US_IX", "US_MI", "US_TN"];

function sendSupervisorEmailReminders() {
  // comma-separated list of state codes as strings
  const statesForQuery = SUPERVISOR_INCLUDED_STATES.map((s) => `"${s}"`).join();
  const supervisorQuery = `WITH supervisors AS (
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

  sendAllLoginReminders(true, supervisorQuery, SUPERVISOR_SETTINGS);
}
