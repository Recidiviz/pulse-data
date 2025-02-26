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
"""A view that maps person_id to the external IDs used in Workflows"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.calculator.query.state.state_specific_query_strings import (
    state_specific_external_id_type,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_ID_TO_EXTERNAL_ID_VIEW_NAME = "person_id_to_external_id"

PERSON_ID_TO_EXTERNAL_ID_DESCRIPTION = (
    """Maps person_id to the external IDs used in Workflows"""
)


PERSON_ID_TO_EXTERNAL_ID_QUERY_TEMPLATE = """
    SELECT
        external_id AS person_external_id,
        person_id,
        state_code,
    FROM `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    WHERE id_type = {state_id_type}
"""

PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=PERSON_ID_TO_EXTERNAL_ID_VIEW_NAME,
    view_query_template=PERSON_ID_TO_EXTERNAL_ID_QUERY_TEMPLATE,
    description=PERSON_ID_TO_EXTERNAL_ID_DESCRIPTION,
    should_materialize=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    state_id_type=state_specific_external_id_type("pei"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER.build_and_print()
