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
"""Helper view providing all possible combinations of superviison dimension values.
Useful for building exhaustive views."""
from typing import Dict, List, Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_constant_dimensions import (
    get_constant_dimensions_unnested,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# for dimensions that have constant values across all states, we need a series of unnest statements
supervision_constant_dimensions: Dict[str, Union[List[int], List[str]]] = {
    # TODO(#11020) Re-enable supervision_level once BE has been updated to handle larger metric files
    "supervision_level": ["ALL"],
    # "supervision_level": [
    #     "MEDIUM",
    #     "INTERSTATE_COMPACT",
    #     "MAXIMUM",
    #     "DIVERSION",
    #     "EXTERNAL_UNKNOWN",
    #     "MINIMUM",
    #     "ALL",
    # ],
    # TODO(#10742): implement violation fields
    "most_severe_violation": ["ALL"],
    "number_of_violations": ["ALL"],
}

# for dimensions whose values vary by state, we will unnest each one separately and then join them
state_specific_dimensions = {
    "supervision_type": {
        "US_ND": [
            "PAROLE",
            "PROBATION",
            "ABSCONSION",
            "ALL",
        ],
        "US_TN": [
            "PAROLE",
            "PROBATION",
            "COMMUNITY_CONFINEMENT",
            "ALL",
        ],
        "US_ID": [
            "PAROLE",
            "PROBATION",
            "INFORMAL_PROBATION",
            "BENCH_WARRANT",
            "ABSCONSION",
            "ALL",
        ],
    }
}

# this will map dimension -> state -> values to UNNEST
values_by_dimension_and_state: Dict[str, Dict[str, str]] = {}

for dimension, state_mapping in state_specific_dimensions.items():
    values_by_state = {}
    for state_code in ENABLED_STATES:
        values_array_contents = [
            f'"{v}"' for v in state_mapping.get(state_code, ["ALL"])
        ]
        values_by_state[
            state_code
        ] = f"UNNEST([{','.join(values_array_contents)}]) AS {dimension}"
    values_by_dimension_and_state[dimension] = values_by_state

# this will contain a series of JOIN clauses, one per dimension
state_specific_joins: List[str] = []

for dimension, state_unnest_mapping in values_by_dimension_and_state.items():
    query = """
    UNION ALL 
    """.join(
        [
            f"""
                SELECT
                    "{state_code}" AS state_code,
                    *
                FROM {unnest_statement}
            """
            for state_code, unnest_statement in state_unnest_mapping.items()
        ]
    )

    state_specific_joins.append(f"FULL OUTER JOIN ({query}) USING (state_code)")

PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME = (
    "pathways_supervision_dimension_combinations"
)

PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_DESCRIPTION = "Helper view providing all possible combinations of supervision dimension values. Useful for building exhaustive views."

PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    constant_dimensions AS (
        SELECT
            *
        FROM {constant_dimensions}
    )
    , districts AS (
        SELECT
            state_code,
            location_name AS district,
        FROM `{project_id}.{dashboard_views_dataset}.pathways_supervision_location_name_map`
        
        UNION ALL

        SELECT
            state_code,
            "ALL" as district,
        FROM UNNEST({enabled_states}) as state_code
    )
    , judicial_districts AS (
        SELECT DISTINCT
            state_code,
            intake_district AS judicial_district,
        FROM `{project_id}.{dashboard_views_dataset}.liberty_to_prison_transitions`

        UNION ALL

        SELECT
            state_code,
            "ALL" as judicial_district,
        FROM UNNEST({enabled_states}) as state_code
    )
    , state_specific_dimensions AS (
        SELECT
            state_code,
            district,
            judicial_district,
            {state_specific_dimension_names},
        FROM districts
        FULL OUTER JOIN judicial_districts USING (state_code)
        {state_specific_joins}
    )

    SELECT
        *
    FROM constant_dimensions
    FULL OUTER JOIN state_specific_dimensions USING (state_code)
"""

PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
    view_query_template=PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_QUERY_TEMPLATE,
    description=PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_DESCRIPTION,
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    constant_dimensions=",".join(
        get_constant_dimensions_unnested(supervision_constant_dimensions)
    ),
    enabled_states=str(ENABLED_STATES),
    state_specific_dimension_names=",".join(state_specific_dimensions.keys()),
    state_specific_joins=" ".join(state_specific_joins),
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_BUILDER.build_and_print()
