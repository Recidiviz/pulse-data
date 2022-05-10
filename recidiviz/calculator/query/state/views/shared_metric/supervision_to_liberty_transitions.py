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
"""People who have transitioned from supervision to liberty by date of release."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import add_age_groups, first_known_location
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    pathways_state_specific_supervision_level,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_NAME = "supervision_to_liberty_transitions"

SUPERVISION_TO_LIBERTY_TRANSITIONS_DESCRIPTION = (
    "Transitions from supervision to liberty by month."
)

SUPERVISION_TO_LIBERTY_TRANSITIONS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        s.state_code,
        s.person_id,
        s.end_date + 1 AS transition_date,
        age_end AS age,
        s.prioritized_race_or_ethnicity,
        {first_known_location} AS district_id,
        IF(compartment_level_2 = 'DUAL', 'PAROLE', compartment_level_2) AS supervision_type,
        {state_specific_supervision_level} AS supervision_level,
        s.gender,
        supervising_officer_external_id_end as supervising_officer,
        supervision_start_date,
        {age_group}
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` s
    LEFT JOIN (
        SELECT
            person_id,
            end_date,
            start_date AS supervision_start_date,
        FROM `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized`
      ) super_sessions
    ON s.person_id = super_sessions.person_id
        AND s.end_date = super_sessions.end_date
    WHERE
        s.state_code IN {enabled_states}
        AND compartment_level_1 = 'SUPERVISION'
        AND COALESCE(compartment_level_2, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN"
        AND outflow_to_level_1 = 'RELEASE'
        AND s.end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 64 MONTH)
        -- (5 years X 12 months) + (3 for 90-day avg) + (1 to capture to beginning of first month) = 64 months
"""

SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_NAME,
    view_query_template=SUPERVISION_TO_LIBERTY_TRANSITIONS_QUERY_TEMPLATE,
    description=SUPERVISION_TO_LIBERTY_TRANSITIONS_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    first_known_location=first_known_location("compartment_location_end"),
    enabled_states=str(tuple(get_pathways_enabled_states())),
    age_group=add_age_groups("age_end"),
    state_specific_supervision_level=pathways_state_specific_supervision_level(
        "s.state_code",
        "correctional_level_end",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_BUILDER.build_and_print()
