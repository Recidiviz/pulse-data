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
"""Creates a view for identifying early discharges from supervision or incarceration sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(##19463): deprecate this view in favor of compartment sessions
# TODO(#10706): explore how many people/sessions we are dropping with this filter
# Limit (in days) for difference between discharge date and session end date
# in order for the discharge to be considered a valid early discharge


# Supported states for early discharges from supervision or incarceration
SUPERVISION_SUPPORTED_STATES = "', '".join(
    ["US_ID", "US_ND", "US_TN", "US_ME", "US_MI", "US_IX"]
)
INCARCERATION_SUPPORTED_STATES = "', '".join(["US_AZ"])

EARLY_DISCHARGE_SESSIONS_VIEW_NAME = "early_discharge_sessions"

EARLY_DISCHARGE_SESSIONS_VIEW_DESCRIPTION = """View of compartment sessions with a flag that identifies early discharges from supervision sessions."""

EARLY_DISCHARGE_SESSIONS_QUERY_TEMPLATE = """
    -- Join separate states datasets and restrict to successful early discharges
    WITH all_ed_sessions AS 
    (
        SELECT * FROM `{project_id}.{analyst_dataset}.us_id_early_discharge_sessions_preprocessing`
        UNION ALL
        SELECT * FROM `{project_id}.{analyst_dataset}.us_nd_early_discharge_sessions_preprocessing`
        UNION ALL
        SELECT * FROM `{project_id}.{analyst_dataset}.us_me_early_discharge_sessions_preprocessing`
        UNION ALL 
        SELECT * FROM `{project_id}.{analyst_dataset}.us_mi_early_discharge_sessions_preprocessing`
        UNION ALL 
        SELECT * FROM `{project_id}.{analyst_dataset}.us_ix_early_discharge_sessions_preprocessing`
        UNION ALL 
        SELECT * FROM `{project_id}.{analyst_dataset}.us_az_early_discharge_sessions_preprocessing`
    )
    SELECT
        sessions.person_id,
        sessions.state_code,
        sessions.session_id,
        sessions.end_date_exclusive AS discharge_date,
        IF(ed.state_code IS NOT NULL,1,0) AS early_discharge,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
    LEFT JOIN all_ed_sessions ed
        USING (state_code, person_id, session_id)
    WHERE 
        ((sessions.state_code IN ('{supervision_supported_states}')
            AND sessions.compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
            AND sessions.outflow_to_level_1 = 'LIBERTY')
        OR
        ((sessions.state_code IN ('{incarceration_supported_states}') AND
            sessions.compartment_level_1 IN ('INCARCERATION', 'INCARCERATION_OUT_OF_STATE')))
            AND sessions.end_date_exclusive IS NOT NULL)
"""

EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=EARLY_DISCHARGE_SESSIONS_VIEW_NAME,
    description=EARLY_DISCHARGE_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=EARLY_DISCHARGE_SESSIONS_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    supervision_supported_states=SUPERVISION_SUPPORTED_STATES,
    incarceration_supported_states=INCARCERATION_SUPPORTED_STATES,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER.build_and_print()
