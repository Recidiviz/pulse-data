# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    PO_REPORT_DATASET,
    SESSIONS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
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
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    GROUP BY 1
)

-- PO-level assignments from static reference table
, po_assignments AS (
    SELECT
        experiment_id,
        state_code,
        officer_external_id,
        variant_id,
        variant_date,
    FROM
        `{project_id}.{static_reference_dataset}.experiment_officer_assignments_materialized`
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
        `{project_id}.{static_reference_dataset}.case_triage_users`
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
        `{project_id}.{po_report_dataset}.sendgrid_po_report_email_events_materialized` events
    INNER JOIN
        `{project_id}.{static_reference_dataset}.po_report_recipients` users
    ON
        events.email = users.email_address
    WHERE
        event = "delivered"
)

-- Union all assignment subqueries
, stacked AS (
    SELECT *
    FROM po_assignments
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
    dataset_id=EXPERIMENTS_DATASET,
    view_id=OFFICER_ASSIGNMENTS_VIEW_NAME,
    view_query_template=OFFICER_ASSIGNMENTS_QUERY_TEMPLATE,
    description=OFFICER_ASSIGNMENTS_VIEW_DESCRIPTION,
    po_report_dataset=PO_REPORT_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
    clustering_fields=["experiment_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_ASSIGNMENTS_VIEW_BUILDER.build_and_print()
