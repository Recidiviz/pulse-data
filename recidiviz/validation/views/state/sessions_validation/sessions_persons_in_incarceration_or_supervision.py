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

"""A view which identifies person_ids in compartment_sessions that do not exist in the
state tables."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSIONS_IN_INCARCERATION_OR_SUPERVISION_VIEW_NAME = (
    "sessions_persons_in_incarceration_or_supervision"
)

SESSIONS_IN_INCARCERATION_OR_SUPERVISION_DESCRIPTION = (
    "Identifies person ids in sessions that do not appear in either incarceration or "
    "supervision state tables."
)

SESSIONS_IN_INCARCERATION_OR_SUPERVISION_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT DISTINCT
        person_id,
        state_code AS region_code,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
    LEFT JOIN (
        SELECT DISTINCT
            person_id,
            state_code,
        FROM `{project_id}.{state_dataset}.state_incarceration_period`
        UNION ALL
        SELECT DISTINCT
            person_id,
            state_code,
        FROM `{project_id}.{state_dataset}.state_supervision_period`
    ) state_tables
    USING (state_code, person_id)
    WHERE state_tables.person_id IS NULL
"""


SESSIONS_IN_INCARCERATION_OR_SUPERVISION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSIONS_IN_INCARCERATION_OR_SUPERVISION_VIEW_NAME,
    view_query_template=SESSIONS_IN_INCARCERATION_OR_SUPERVISION_QUERY_TEMPLATE,
    description=SESSIONS_IN_INCARCERATION_OR_SUPERVISION_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    state_dataset=state_dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSIONS_IN_INCARCERATION_OR_SUPERVISION_VIEW_BUILDER.build_and_print()
