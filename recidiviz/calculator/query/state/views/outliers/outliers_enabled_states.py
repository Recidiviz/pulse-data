#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
#  =============================================================================
"""Lists the states for which outliers is enabled."""
import os
from typing import List

import yaml

import recidiviz
from recidiviz.common.constants.states import StateCode

yaml_path = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "tools/deploy/terraform/config/outliers_enabled_states.yaml",
)

_outliers_enabled_states: List[str] = []


def get_outliers_enabled_states() -> List[str]:
    global _outliers_enabled_states

    if _outliers_enabled_states:
        return _outliers_enabled_states

    with open(yaml_path, "r", encoding="utf-8") as ymlfile:
        outliers_enabled_states: List[str] = yaml.full_load(ymlfile)

    _outliers_enabled_states = [
        StateCode[state_code].value
        for state_code in outliers_enabled_states
        if StateCode.is_state_code(state_code)
    ]

    return _outliers_enabled_states


def get_outliers_enabled_states_for_bigquery() -> List[str]:
    # Only US_IX data should be used while the US_IX -> US_ID migration is incomplete.
    # The US_IX value will be overriden to US_ID during export to GCS.
    return [state for state in get_outliers_enabled_states() if state != "US_ID"] + [
        StateCode.US_IX.value
    ]
