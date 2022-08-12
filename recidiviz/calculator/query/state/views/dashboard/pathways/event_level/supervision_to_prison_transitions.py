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
"""People who have transitioned from supervision to prison by date of reincarceration."""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    first_known_location,
    get_binned_time_period_months,
    length_of_stay_month_groups,
    non_active_supervision_levels,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_supervision_last_updated_date,
    pathways_state_specific_supervision_level,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_NAME = "supervision_to_prison_transitions"

SUPERVISION_TO_PRISON_TRANSITIONS_DESCRIPTION = (
    "Transitions from supervision to prison by month."
)

SUPERVISION_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE = """
    WITH base_data AS (
        SELECT
            sessions.state_code,
            sessions.person_id,
            sessions.end_date + 1 AS transition_date,
            super_sessions.start_date AS supervision_start_date,
            IF(sessions.compartment_level_2 = 'DUAL', 'PAROLE', sessions.compartment_level_2) AS supervision_type,
            IFNULL(
                IF(
                    {state_specific_supervision_level} IN {non_active_supervision_levels},
                    {supervision_sessions_state_specific_supervision_level},
                    {state_specific_supervision_level}
                ),
                "EXTERNAL_UNKNOWN"
             ) AS supervision_level,
            sessions.age_end AS age,
            {age_group}
            sessions.gender,
            sessions.prioritized_race_or_ethnicity,
            sessions.supervising_officer_external_id_end AS supervising_officer,
            {first_known_location} AS level_1_location_external_id,
        FROM
            `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
        LEFT JOIN (
            SELECT
                person_id,
                end_date,
                start_date,
            FROM `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized`
          ) super_sessions
        ON sessions.person_id = super_sessions.person_id
            AND sessions.end_date = super_sessions.end_date
        LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` supervision_sessions
        ON sessions.person_id=supervision_sessions.person_id
            AND sessions.end_date BETWEEN supervision_sessions.start_date AND supervision_sessions.end_date
        WHERE
            sessions.state_code IN {enabled_states}
            AND compartment_level_1 = 'SUPERVISION'
            AND COALESCE(compartment_level_2, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN"
            AND outflow_to_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE", "PENDING_CUSTODY")
            AND sessions.end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 64 MONTH)
            -- (5 years X 12 months) + (3 for 90-day avg) + (1 to capture to beginning of first month) = 64 months
    ), 
    transitions AS (
        SELECT 
            base_data.state_code,
            base_data.person_id,
            base_data.transition_date,
            EXTRACT(YEAR FROM base_data.transition_date) AS year,
            EXTRACT(MONTH FROM base_data.transition_date) AS month,
            base_data.supervision_type,
            base_data.supervision_level,
            base_data.age,
            base_data.age_group,
            base_data.gender,
            base_data.prioritized_race_or_ethnicity AS race,
            base_data.supervising_officer,
            base_data.supervision_start_date,
            UPPER(COALESCE(location.location_name, base_data.level_1_location_external_id, 'EXTERNAL_UNKNOWN'))
                AS supervision_district,
            {transition_time_period} AS time_period,
            {length_of_stay_months_grouped} AS length_of_stay,
        FROM base_data
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_supervision_location_name_map` location
            ON base_data.state_code = location.state_code
            AND base_data.level_1_location_external_id = location.location_id
    )
    SELECT {columns} FROM transitions
"""

SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER = WithMetadataQueryBigQueryViewBuilder(
    metadata_query=get_pathways_supervision_last_updated_date(),
    delegate=SelectedColumnsBigQueryViewBuilder(
        columns=[
            "state_code",
            "person_id",
            "transition_date",
            "year",
            "month",
            "supervision_type",
            "supervision_level",
            "age",
            "age_group",
            "gender",
            "race",
            "supervising_officer",
            "supervision_start_date",
            "supervision_district",
            "time_period",
            "length_of_stay",
        ],
        dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
        view_id=SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_NAME,
        view_query_template=SUPERVISION_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE,
        description=SUPERVISION_TO_PRISON_TRANSITIONS_DESCRIPTION,
        sessions_dataset=dataset_config.SESSIONS_DATASET,
        age_group=add_age_groups("sessions.age_end"),
        first_known_location=first_known_location("compartment_location_end"),
        non_active_supervision_levels=non_active_supervision_levels(),
        enabled_states=str(tuple(get_pathways_enabled_states())),
        state_specific_supervision_level=pathways_state_specific_supervision_level(
            "sessions.state_code",
            "sessions.correctional_level_end",
        ),
        supervision_sessions_state_specific_supervision_level=pathways_state_specific_supervision_level(
            "supervision_sessions.state_code",
            "supervision_sessions.most_recent_active_supervision_level",
        ),
        transition_time_period=get_binned_time_period_months("transition_date"),
        length_of_stay_months_grouped=length_of_stay_month_groups(
            "DATE_DIFF(base_data.transition_date, base_data.supervision_start_date, MONTH)"
        ),
        dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER.build_and_print()
