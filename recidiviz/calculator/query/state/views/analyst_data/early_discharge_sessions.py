# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Creates a view for identifying early discharges from supervision sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#10706): explore how many people/sessions we are dropping with this filter
# Limit (in days) for difference between discharge date and session end date
# in order for the discharge to be considered a valid early discharge
DISCHARGE_SESSION_DIFF_DAYS = "7"

# States currently supported
SUPPORTED_STATES = (
    "US_ID",
    "US_ND",
)

EARLY_DISCHARGE_SESSIONS_VIEW_NAME = "early_discharge_sessions"

EARLY_DISCHARGE_SESSIONS_VIEW_DESCRIPTION = """View of compartment sessions with a flag that identifies early discharges from supervision sessions."""

EARLY_DISCHARGE_SESSIONS_QUERY_TEMPLATE = """
    -- Join separate states datasets and restrict to successful early discharges
    WITH all_ed_sessions AS (
        SELECT *
        FROM (
            SELECT * FROM `{project_id}.{analyst_dataset}.us_id_early_discharge_sessions_preprocessing`
            UNION ALL
            SELECT * FROM `{project_id}.{analyst_dataset}.us_nd_early_discharge_sessions_preprocessing`
        )
        -- Only count sessions ending in release where the discharge date is within a specified number of days of session end date
        WHERE outflow_to_level_1 = 'LIBERTY'
            AND discharge_to_session_end_days <= CAST({discharge_session_diff_days} AS INT64)
    ),
    -- Create a flag for early discharge
    final_sessions AS (
        SELECT
            sessions.*,
            ed.ed_date,
            CASE WHEN ed.state_code IS NOT NULL THEN 1 ELSE 0 END AS early_discharge,
        FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
        LEFT JOIN all_ed_sessions ed
            USING (state_code, person_id, session_id)
        -- Restrict final output to only include states where we have early discharge data
        WHERE sessions.state_code IN ('{supported_states}')
    )

    SELECT * FROM final_sessions
"""

EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=EARLY_DISCHARGE_SESSIONS_VIEW_NAME,
    description=EARLY_DISCHARGE_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=EARLY_DISCHARGE_SESSIONS_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    supported_states="', '".join(SUPPORTED_STATES),
    discharge_session_diff_days=DISCHARGE_SESSION_DIFF_DAYS,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER.build_and_print()
