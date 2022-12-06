#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
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
"""Lists the states for which pathways is enabled."""
import os
from typing import List

import yaml

import recidiviz
from recidiviz.common.constants.states import StateCode
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, in_gcp_production

yaml_path = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "tools/deploy/terraform/config/pathways_enabled_states.yaml",
)

_pathways_enabled_states: List[str] = []

PATHWAYS_OFFLINE_DEMO_STATE = StateCode.US_OZ


def get_pathways_enabled_states() -> List[str]:
    global _pathways_enabled_states

    if _pathways_enabled_states:
        return _pathways_enabled_states

    with open(yaml_path, "r", encoding="utf-8") as ymlfile:
        pathways_enabled_states: List[str] = yaml.full_load(ymlfile)

    _pathways_enabled_states = [
        StateCode[state_code].value
        for state_code in pathways_enabled_states
        if StateCode.is_state_code(state_code)
    ]

    # TODO(#15073): This checks both the RECIDIVIZ_ENV environment variable
    # and the project id because the environment variable won't be set in
    # local scripts, like when we are running migrations. We should consider
    # getting rid of the environment variable entirely and just always
    # using project id throughout the codebase.
    # Hints are not logged as this is called at import-time
    if not in_gcp_production() and not metadata.running_against(
        GCP_PROJECT_PRODUCTION, log_hint=False
    ):
        # Add demo state for demo and offline modes
        _pathways_enabled_states += [PATHWAYS_OFFLINE_DEMO_STATE.value]

    return _pathways_enabled_states


def get_pathways_enabled_states_for_bigquery() -> List[str]:
    # This can't be automatically detected based on environment because BigQuery views may be
    # compiled locally (where we want it to include US_IX), but DB migrations are also run locally
    # (where we don't want it to include US_IX).
    return get_pathways_enabled_states() + [StateCode.US_IX.value]
