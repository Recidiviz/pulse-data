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
"""People who have transitioned from prison to supervision"""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import add_age_groups
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_NAME = "prison_to_supervision_transitions"

PRISON_TO_SUPERVISION_TRANSITIONS_DESCRIPTION = (
    "Transitions from prison to supervision by month."
)

PRISON_TO_SUPERVISION_TRANSITIONS_QUERY_TEMPLATE = """
    WITH sessions_data AS (
        SELECT
            sessions.state_code,
            sessions.person_id,
            sessions.end_date + 1 AS transition_date,
            EXTRACT(YEAR FROM sessions.end_date + 1) AS year,
            EXTRACT(MONTH FROM sessions.end_date + 1) AS month,
            sessions.age_end AS age,
            {age_group}
            sessions.gender,
            prioritized_race_or_ethnicity AS race,
            SPLIT(sessions.compartment_location_end, "|")[OFFSET(0)] AS level_1_location_external_id,
        FROM
            `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
        WHERE
            state_code IN {enabled_states}
            AND compartment_level_1 = 'INCARCERATION'
            AND outflow_to_level_1 IN {outflow_compartments}
            AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 64 MONTH)
            -- (5 years X 12 months) + (3 for 90-day avg) + (1 to capture to beginning of first month) = 64 months
    )
    , all_rows AS (
        SELECT
            sessions_data.state_code,
            sessions_data.person_id,
            sessions_data.transition_date,
            sessions_data.year,
            sessions_data.month,
            sessions_data.age,
            sessions_data.age_group,
            sessions_data.gender,
            sessions_data.race,
            IFNULL(aggregating_location_id, level_1_location_external_id) AS facility,
        FROM sessions_data
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` location
            ON sessions_data.state_code = location.state_code 
            AND level_1_location_external_id = location_id
    )
    SELECT {columns}
    FROM all_rows
    WHERE {facility_filter}
"""

PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_NAME,
    view_query_template=PRISON_TO_SUPERVISION_TRANSITIONS_QUERY_TEMPLATE,
    description=PRISON_TO_SUPERVISION_TRANSITIONS_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    outflow_compartments='("SUPERVISION")',
    age_group=add_age_groups("sessions.age_end"),
    enabled_states=str(tuple(get_pathways_enabled_states())),
    facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
    columns=[
        "transition_date",
        "year",
        "month",
        "person_id",
        "age_group",
        "age",
        "gender",
        "race",
        "facility",
        "state_code",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER.build_and_print()
