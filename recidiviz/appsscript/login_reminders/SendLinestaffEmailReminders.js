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

const LINESTAFF_SETTINGS = {
  EXCLUDED_DISTRICTS: ["NOT_APPLICABLE", "EXTERNAL_UNKNOWN"],

  EMAIL_FROM_ALIAS: "email-reports@recidiviz.org",
  FEEDBACK_EMAIL: "feedback@recidiviz.org",

  EMAIL_SUBJECT: "Recidiviz missed you this month!",
  RECIDIVIZ_LINK: "https://dashboard.recidiviz.org/",
  RECIDIVIZ_LINK_TEXT: "Login to Recidiviz",
};

const LINESTAFF_INCLUDED_STATES = ["US_IX", "US_ME", "US_MI", "US_ND", "US_TN"];

// =============================================================================

// comma-separated list of state codes as strings
const linestaffStatesForQuery = LINESTAFF_INCLUDED_STATES.map(
  (s) => `"${s}"`
).join();
const LINESTAFF_QUERY = `WITH officers AS (
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
),
latest_eligible_opportunities AS (
    SELECT
        surfaceable_population.state_code,
        staff.email AS officer_email,
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
-- Note: If the order of the columns changes, we must account for the change within EmailReminderHelpers
SELECT
    officers.state_code,
    officers.officer_external_id,
    officers.officer_name,
    officers.officer_email,
    officers.district,
    latest_eligible_opportunities.total_opportunities,
FROM officers
LEFT JOIN latest_eligible_opportunities
  ON LOWER(latest_eligible_opportunities.officer_email) = LOWER(officers.officer_email)
WHERE 
-- We only want to email people who have eligible opportunities in the tool
  latest_eligible_opportunities.total_opportunities IS NOT NULL
  AND officers.state_code IN ( ${linestaffStatesForQuery} )`;

function sendLinestaffEmailReminders_() {
  sendAllLoginReminders(
    false,
    LINESTAFF_QUERY,
    LINESTAFF_SETTINGS,
    LINESTAFF_INCLUDED_STATES
  );
}
