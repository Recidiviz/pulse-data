# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Creates the view builder and view for officer experiment assignments."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments_metadata.dataset_config import (
    EXPERIMENTS_METADATA_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFICER_ASSIGNMENTS_VIEW_NAME = "officer_assignments"

OFFICER_ASSIGNMENTS_VIEW_DESCRIPTION = (
    "Tracks assignments of experiment/policy/program variants to officers in each "
    "experiment."
)

OFFICER_ASSIGNMENTS_QUERY_TEMPLATE = """
-- last day data observable in sessions
WITH last_day_of_data AS (
    SELECT
        state_code,
        MIN(last_day_of_data) AS last_day_of_data,
    FROM
        `{project_id}.sessions.compartment_sessions_materialized`
    GROUP BY 1
)

-- officer-level assignments from static reference table
, officer_assignments AS (
    SELECT
        experiment_id,
        state_code,
        unit_id AS officer_external_id,
        variant_id,
        variant_date,
    FROM
        `{project_id}.experiments_metadata.experiment_assignments_materialized`
    WHERE
        unit_type = "OFFICER"
)

-- state-level assignments passed through to officers, e.g. statewide feature rollouts
, state_assignments AS (
    SELECT DISTINCT
      experiment_id,
      a.state_code,
      supervising_officer_external_id AS officer_external_id,
      variant_id,
      variant_date,
    FROM
      `{project_id}.experiments_metadata.state_assignments_materialized` a
    INNER JOIN
      `{project_id}.sessions.supervision_officer_sessions_materialized` b
    ON
      a.state_code = b.state_code
      AND variant_date BETWEEN b.start_date AND IFNULL(b.end_date, "9999-01-01")
)

-- When officers first given access to Case Triage
, case_triage_access AS (
    SELECT
        "CASE_TRIAGE_ACCESS" AS experiment_id,
        state_code,
        officer_external_id,
        "RECEIVED_ACCESS" AS variant_id,
        received_access AS variant_date,
    FROM
        `{project_id}.static_reference_tables.case_triage_users`
)

-- When officers receive first monthly report
, first_monthly_report AS (
    SELECT
        "FIRST_MONTHLY_REPORT" AS experiment_id,
        state_code,
        officer_external_id,
        "RECEIVED_EMAIL" AS variant_id,
        DATE(events.event_datetime) AS variant_date,
    FROM
        `{project_id}.sendgrid_email_data.sendgrid_po_report_email_events_2023_08_03_backup` events
    INNER JOIN
        `{project_id}.static_reference_tables.po_report_recipients` users
    ON
        events.email = users.email_address
    WHERE
        event = "delivered"
)

-- Union all assignment subqueries
, stacked AS (
    SELECT *
    FROM officer_assignments
    UNION ALL
    SELECT *
    FROM state_assignments
    UNION ALL
    SELECT *
    FROM case_triage_access
    UNION ALL
    SELECT *
    FROM first_monthly_report
)

-- Add state-level last day data observed
SELECT *
FROM stacked
INNER JOIN last_day_of_data USING(state_code)
"""

OFFICER_ASSIGNMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_METADATA_DATASET,
    view_id=OFFICER_ASSIGNMENTS_VIEW_NAME,
    view_query_template=OFFICER_ASSIGNMENTS_QUERY_TEMPLATE,
    description=OFFICER_ASSIGNMENTS_VIEW_DESCRIPTION,
    should_materialize=True,
    clustering_fields=["experiment_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_ASSIGNMENTS_VIEW_BUILDER.build_and_print()
