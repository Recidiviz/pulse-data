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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import add_age_groups
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_NAME = "prison_to_supervision_transitions"

PRISON_TO_SUPERVISION_TRANSITIONS_DESCRIPTION = (
    "Transitions from prison to supervision by month."
)

PRISON_TO_SUPERVISION_TRANSITIONS_QUERY_TEMPLATE = """
    SELECT
        sessions.state_code,
        sessions.person_id,
        sessions.end_date AS transition_date,
        sessions.age_end AS age,
        {age_group}
        sessions.gender,
        SPLIT(sessions.compartment_location_end, "|")[OFFSET(0)] AS level_1_location_external_id,
    FROM
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
    WHERE
        state_code IN {enabled_states}
        AND compartment_level_1 = 'INCARCERATION'
        AND outflow_to_level_1 IN {outflow_compartments}
        AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 64 MONTH)
        -- (5 years X 12 months) + (3 for 90-day avg) + (1 to capture to beginning of first month) = 64 months"""

PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_NAME,
    view_query_template=PRISON_TO_SUPERVISION_TRANSITIONS_QUERY_TEMPLATE,
    description=PRISON_TO_SUPERVISION_TRANSITIONS_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    outflow_compartments='("SUPERVISION")',
    age_group=add_age_groups("sessions.age_end"),
    enabled_states=str(tuple(ENABLED_STATES)),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER.build_and_print()
