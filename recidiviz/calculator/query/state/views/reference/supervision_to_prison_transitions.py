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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import add_age_groups
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_NAME = "supervision_to_prison_transitions"

SUPERVISION_TO_PRISON_TRANSITIONS_DESCRIPTION = (
    "Transitions from supervision to prison by month."
)

SUPERVISION_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE = """
    SELECT
        sessions.state_code,
        sessions.person_id,
        sessions.end_date + 1 AS transition_date,
        IF(sessions.compartment_level_2 = 'DUAL', 'PAROLE', sessions.compartment_level_2) AS supervision_type,
        SPLIT(sessions.correctional_level_end, "|")[OFFSET(0)] AS supervision_level,
        sessions.age_end AS age,
        {age_group}
        sessions.gender,
        sessions.prioritized_race_or_ethnicity,
        sessions.supervising_officer_external_id_end AS supervising_officer,
        SPLIT(sessions.compartment_location_end, "|")[OFFSET(0)] AS level_1_location_external_id,
        super_sessions.start_date AS supervision_start_date,
    FROM
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
    LEFT JOIN (
        SELECT
            person_id,
            end_date,
            start_date,
        FROM `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized`
      ) super_sessions USING (person_id, end_date)
    WHERE
        state_code IN {enabled_states}
        AND compartment_level_1 = 'SUPERVISION'
        AND compartment_level_2 IN ('PAROLE', 'PROBATION', 'INFORMAL_PROBATION', 'BENCH_WARRANT', 'DUAL', 'ABSCONSION')
        AND outflow_to_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE", "PENDING_CUSTODY")
        AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 64 MONTH)
        -- (5 years X 12 months) + (3 for 90-day avg) + (1 to capture to beginning of first month) = 64 months"""

SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_NAME,
    view_query_template=SUPERVISION_TO_PRISON_TRANSITIONS_QUERY_TEMPLATE,
    description=SUPERVISION_TO_PRISON_TRANSITIONS_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    age_group=add_age_groups("sessions.age_end"),
    enabled_states=str(tuple(ENABLED_STATES)),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER.build_and_print()
