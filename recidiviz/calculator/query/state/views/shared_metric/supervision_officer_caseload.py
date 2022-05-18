#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
"""Number of people on an officer's caseload in a specific time period matching dimension combinations."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    filter_to_enabled_states,
    get_binned_time_period_months,
    non_active_supervision_levels,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import DASHBOARD_VIEWS_DATASET
from recidiviz.calculator.query.state.state_specific_query_strings import (
    pathways_state_specific_supervision_level,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_CASELOAD_VIEW_NAME = "supervision_officer_caseload"

SUPERVISION_OFFICER_CASELOAD_DESCRIPTION = "Caseload per officer per time period."

SUPERVISION_OFFICER_CASELOAD_QUERY_TEMPLATE = """
    /* {description} */
    WITH cte AS (
        SELECT DISTINCT
            s.state_code,
            s.dataflow_session_id,
            s.person_id,
            s.start_date,
            s.end_date,
            {transition_time_period} AS time_period,
            supervising_officer_external_id,
            session_attributes.correctional_level,
            session_attributes.compartment_level_2,
            IFNULL(name_map.location_name,session_attributes.supervision_office) AS district,
        FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized` s,
        UNNEST (session_attributes) session_attributes
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_supervision_location_name_map` name_map
            ON s.state_code = name_map.state_code
            AND session_attributes.supervision_office = name_map.location_id
        WHERE session_attributes.compartment_level_1 = 'SUPERVISION' 
    )
    , row_detail AS (
        SELECT
            cte.state_code,
            cte.person_id,
            cte.start_date,
            cte.end_date,
            cte.time_period,
            cte.district,
            cte.supervising_officer_external_id AS supervising_officer,
            cs.gender,
            cs.prioritized_race_or_ethnicity AS race,
            {age_group}
            IF(cte.compartment_level_2 = 'DUAL', 'PAROLE', cte.compartment_level_2) AS supervision_type,
            IF({state_specific_supervision_level} IN {non_active_supervision_levels},
                {supervision_sessions_state_specific_supervision_level}, {state_specific_supervision_level}) AS supervision_level,
        FROM cte
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` cs
            ON cte.state_code = cs.state_code
            AND cte.person_id = cs.person_id
            AND cte.dataflow_session_id >= cs.dataflow_session_id_start
            AND cte.dataflow_session_id <= cs.dataflow_session_id_end
        LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` supervision_sessions
            ON cs.person_id=supervision_sessions.person_id
                AND cs.end_date BETWEEN supervision_sessions.start_date AND supervision_sessions.end_date
        AND supervising_officer_external_id IS NOT NULL
    )
    SELECT 
        state_code,
        time_period,
        district,
        supervising_officer,
        supervision_level,
        gender,
        race,
        age_group,
        supervision_type,
        COUNT(distinct person_id) as caseload
    FROM row_detail,
    UNNEST ([gender, 'ALL']) AS gender,
    UNNEST ([supervision_type, 'ALL']) AS supervision_type,
    UNNEST ([supervision_level, 'ALL']) AS supervision_level,
    UNNEST ([age_group, 'ALL']) AS age_group,
    UNNEST ([race, "ALL"]) AS race,
    UNNEST ([district, "ALL"]) AS district,
    UNNEST([supervising_officer, "ALL"]) AS supervising_officer
    {filter_to_enabled_states}
    AND time_period IS NOT NULL
    AND supervising_officer IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9

"""

SUPERVISION_OFFICER_CASELOAD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_CASELOAD_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_CASELOAD_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_CASELOAD_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    dashboards_dataset=DASHBOARD_VIEWS_DATASET,
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=get_pathways_enabled_states()
    ),
    transition_time_period=get_binned_time_period_months(
        "end_date", "WHEN s.end_date is NULL THEN 'months_0_6'"
    ),
    age_group=add_age_groups("cs.age_end"),
    state_specific_supervision_level=pathways_state_specific_supervision_level(
        "cte.state_code",
        "cte.correctional_level",
    ),
    supervision_sessions_state_specific_supervision_level=pathways_state_specific_supervision_level(
        "supervision_sessions.state_code",
        "supervision_sessions.most_recent_active_supervision_level",
    ),
    non_active_supervision_levels=non_active_supervision_levels(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_CASELOAD_VIEW_BUILDER.build_and_print()
