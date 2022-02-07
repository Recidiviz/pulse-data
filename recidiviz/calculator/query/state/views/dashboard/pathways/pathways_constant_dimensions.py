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

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.common.constants.shared_enums.person_characteristics import (
    Ethnicity,
    Race,
)

# for dimensions that have constant values across all states, we need a series of unnest statements
shared_constant_dimensions: Dict[str, Union[List[int], List[str]]] = {
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
        Ethnicity.HISPANIC.value,
        Race.OTHER.value,
        "ALL",
    ],
}


def get_constant_dimensions_unnested(
    constant_dimensions: Dict[str, Union[List[int], List[str]]]
) -> List[str]:
    constant_dimensions_unnested: List[str] = []

    all_dimensions = {**shared_constant_dimensions, **constant_dimensions}
    for dimension, values in all_dimensions.items():
        # not necessary to check the entire list, all members should be the same type
        if isinstance(values[0], str):
            values = [f'"{value}"' for value in values]
        else:
            values = [str(value) for value in values]
        constant_dimensions_unnested.append(
            f"UNNEST([{','.join(values)}]) AS {dimension}"
        )

    return constant_dimensions_unnested
