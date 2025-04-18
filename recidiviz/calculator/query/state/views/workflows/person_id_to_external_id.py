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
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    state_specific_incarceration_external_id_type,
    state_specific_supervision_external_id_type,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_ID_TO_EXTERNAL_ID_VIEW_NAME = "person_id_to_external_id"

PERSON_ID_TO_EXTERNAL_ID_DESCRIPTION = (
    """Maps person_id to the external IDs used in Workflows"""
)


PERSON_ID_TO_EXTERNAL_ID_QUERY_TEMPLATE = f"""
    SELECT
        external_id AS person_external_id,
        person_id,
        state_code,
        "SUPERVISION" AS system_type,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    WHERE id_type = {{supervision_state_id_type}}
    AND state_code != 'US_NE'

    UNION ALL

    -- Nebraska clients may have multiple external IDs, so pick the one for the current supervision period
    SELECT
        pei.external_id AS person_external_id,
        pei.person_id,
        pei.state_code,
        "SUPERVISION" AS system_type,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
    ON 
        pei.state_code = sp.state_code
        AND pei.person_id = sp.person_id
        AND pei.external_id = SPLIT(sp.external_id ,"-")[OFFSET(0)]
        AND {today_between_start_date_and_nullable_end_date_clause('sp.start_date', 'sp.termination_date')}
    WHERE pei.state_code = 'US_NE'

    UNION ALL

    SELECT
        external_id AS person_external_id,
        person_id,
        state_code,
        "INCARCERATION" AS system_type,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
    WHERE id_type = {{incarceration_state_id_type}}
"""

PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=PERSON_ID_TO_EXTERNAL_ID_VIEW_NAME,
    view_query_template=PERSON_ID_TO_EXTERNAL_ID_QUERY_TEMPLATE,
    description=PERSON_ID_TO_EXTERNAL_ID_DESCRIPTION,
    should_materialize=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    supervision_state_id_type=state_specific_supervision_external_id_type("pei"),
    incarceration_state_id_type=state_specific_incarceration_external_id_type("pei"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER.build_and_print()
