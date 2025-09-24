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
/* Apps Script for sending email reminders to supervision line staff. */

const SUPERVISION_LINESTAFF_INCLUDED_STATES = [
  "US_IA",
  "US_IX",
  "US_MI",
  "US_ND",
  "US_NE",
  "US_PA",
  "US_TN",
  "US_UT",
];

// =============================================================================

// comma-separated list of state codes as strings
const supervisionLinestaffStatesForQuery =
  SUPERVISION_LINESTAFF_INCLUDED_STATES.map((s) => `"${s}"`).join();

// Note: If the order of the columns in the query changes, we must
// account for the change within EmailReminderHelpers.
const SUPERVISION_LINESTAFF_QUERY = `SELECT 
  state_code,
  officer_id,
  officer_name,
  email_address,
  location_name,
  total_opportunities,
  eligible_opportunities,
  almost_eligible_opportunities,
  eligible_clients_by_opportunity,
  almost_eligible_clients_by_opportunity,
  urgent_clients_by_opportunity
FROM
  \`recidiviz-123.user_metrics.workflows_supervision_user_available_actions_materialized\`
WHERE
  state_code IN (${supervisionLinestaffStatesForQuery})
ORDER BY state_code`;

function sendSupervisionLinestaffEmailReminders_() {
  sendAllLoginReminders(
    SUPERVISION_LINESTAFF,
    SUPERVISION_LINESTAFF_QUERY,
    EMAIL_SETTINGS,
    SUPERVISION_LINESTAFF_INCLUDED_STATES
  );
}
