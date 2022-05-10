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
"""Helper view providing all possible combinations of dimension values. Useful for building exhaustive views."""
from typing import Dict, List, Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_constant_dimensions import (
    get_constant_dimensions_unnested,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_supervision_dimension_combinations import (
    state_specific_joins,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_NAME = (
    "pathways_prison_dimension_combinations"
)

PATHWAYS_PRISON_DIMENSION_COMBINATIONS_DESCRIPTION = "Helper view providing all possible combinations of incarceration dimension values. Useful for building exhaustive views."

# for dimensions that have constant values across all states, we need a series of unnest statements
prison_constant_dimensions: Dict[str, Union[List[int], List[str]]] = {
    "admission_reason": [
        "REVOCATION",
        "NEW_ADMISSION",
        "RETURN_FROM_ESCAPE",
        "EXTERNAL_UNKNOWN",
        "TRANSFER",
        "TRANSFER_FROM_OTHER_JURISDICTION",
        "INTERNAL_UNKNOWN",
        "ADMITTED_IN_ERROR",
        "STATUS_CHANGE",
        "ALL",
    ],
}

PATHWAYS_PRISON_DIMENSION_COMBINATIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    constant_dimensions AS (
        SELECT
            *
        FROM {constant_dimensions}
    )
    , facilities AS (
        SELECT
            state_code,
            aggregating_location_id AS facility,
        FROM `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map`

        UNION ALL

        SELECT
            state_code,
            "ALL" as facility,
        FROM UNNEST({enabled_states}) as state_code
    )
    , state_specific_dimensions AS (
        SELECT
            state_code,
            facility
        FROM facilities
        {state_specific_joins}
    )

    SELECT
        *
    FROM constant_dimensions
    FULL OUTER JOIN state_specific_dimensions USING (state_code)
"""

PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_NAME,
    view_query_template=PATHWAYS_PRISON_DIMENSION_COMBINATIONS_QUERY_TEMPLATE,
    description=PATHWAYS_PRISON_DIMENSION_COMBINATIONS_DESCRIPTION,
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    constant_dimensions=",".join(
        get_constant_dimensions_unnested(prison_constant_dimensions)
    ),
    enabled_states=str(get_pathways_enabled_states()),
    state_specific_joins=" ".join(state_specific_joins),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_BUILDER.build_and_print()
