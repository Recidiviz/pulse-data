#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
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
"""Lists the states for which workflows is enabled."""
import os
from typing import List

import yaml

import recidiviz
from recidiviz.common.constants.states import StateCode

yaml_path = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "tools/deploy/terraform/config/workflows_enabled_states.yaml",
)

_workflows_enabled_states: List[str] = []


def get_workflows_enabled_states() -> List[str]:
    global _workflows_enabled_states

    if _workflows_enabled_states:
        return _workflows_enabled_states

    with open(yaml_path, "r", encoding="utf-8") as ymlfile:
        workflows_enabled_states: List[str] = yaml.full_load(ymlfile)

    _workflows_enabled_states = [
        StateCode[state_code].value
        for state_code in workflows_enabled_states
        if StateCode.is_state_code(state_code)
    ]

    return _workflows_enabled_states
