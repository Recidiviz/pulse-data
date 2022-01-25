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
from datetime import date
from typing import Dict, List, Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# for dimensions that have constant values across all states, we need a series of unnest statements
constant_dimensions: Dict[str, Union[List[int], List[str]]] = {
    "state_code": ENABLED_STATES,
    "year": [date.today().year - y for y in range(6)],
    "month": list(range(1, 13)),
    "gender": ["MALE", "FEMALE", "ALL"],
    "age_group": [
        "<25",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60+",
        "ALL",
    ],
    "race": [
        Race.ASIAN.value,
        Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER.value,
        Race.BLACK.value,
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE.value,
        Race.WHITE.value,
        Race.OTHER.value,
        "ALL",
    ],
    "supervision_level": ["ALL"],
    # TODO(#10742): implement violation fields
    "most_severe_violation": ["ALL"],
    "number_of_violations": ["ALL"],
}

constant_dimensions_unnested: List[str] = []

for dimension, values in constant_dimensions.items():
    # not necessary to check the entire list, all members should be the same type
    if isinstance(values[0], str):
        values = [f'"{value}"' for value in values]
    else:
        values = [str(value) for value in values]
    constant_dimensions_unnested.append(f"UNNEST([{','.join(values)}]) AS {dimension}")

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

PATHWAYS_DIMENSION_COMBINATIONS_VIEW_NAME = "pathways_dimension_combinations"

PATHWAYS_DIMENSION_COMBINATIONS_DESCRIPTION = "Helper view providing all possible combinations of dimension values. Useful for building exhaustive views."

PATHWAYS_DIMENSION_COMBINATIONS_QUERY_TEMPLATE = """
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
    , state_specific_dimensions AS (
        SELECT
            state_code,
            district,
            {state_specific_dimension_names},
        FROM districts
        {state_specific_joins}
    )

    SELECT
        *
    FROM constant_dimensions
    FULL OUTER JOIN state_specific_dimensions USING (state_code)
"""

PATHWAYS_DIMENSION_COMBINATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PATHWAYS_DIMENSION_COMBINATIONS_VIEW_NAME,
    view_query_template=PATHWAYS_DIMENSION_COMBINATIONS_QUERY_TEMPLATE,
    description=PATHWAYS_DIMENSION_COMBINATIONS_DESCRIPTION,
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    constant_dimensions=",".join(constant_dimensions_unnested),
    enabled_states=str(ENABLED_STATES),
    state_specific_dimension_names=",".join(state_specific_dimensions.keys()),
    state_specific_joins=" ".join(state_specific_joins),
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PATHWAYS_DIMENSION_COMBINATIONS_VIEW_BUILDER.build_and_print()
