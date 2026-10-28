# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Loads a state's law regimes, and the named conditions they share, from the
YAML files in this package."""

import os
import re

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator import rules as rules_package
from recidiviz.sentence_calculator.models import state_inputs
from recidiviz.sentence_calculator.models.conditions import Condition
from recidiviz.sentence_calculator.models.rules import LawRegime, parse_state_conditions

# Regime names become file names, so they are limited to characters that can't
# escape the state's directory.
_REGIME_NAME_REGEX = re.compile(r"[A-Za-z0-9_]+")

# The file in a state's directory that holds the named conditions every regime
# for the state can refer to.
STATE_CONDITIONS_FILE_NAME = "conditions.yaml"


def _state_rules_dir(state_code: StateCode) -> str:
    return os.path.join(
        os.path.dirname(rules_package.__file__), state_code.value.lower()
    )


def law_regime_yaml_path(*, state_code: StateCode, regime_name: str) -> str:
    """Returns the path of the YAML file defining regime_name for state_code, e.g.
    .../rules/us_nv/sb_413.yaml for US_NV and SB_413."""
    if not _REGIME_NAME_REGEX.fullmatch(regime_name):
        raise ValueError(
            f"Invalid law regime name [{regime_name}]. Regime names may only "
            f"contain letters, digits, and underscores."
        )
    return os.path.join(_state_rules_dir(state_code), f"{regime_name.lower()}.yaml")


def load_state_conditions(state_code: StateCode) -> dict[str, Condition]:
    """Returns the named conditions every regime for state_code can refer to,
    keyed by name, or an empty dict if the state has no conditions file."""
    yaml_path = os.path.join(_state_rules_dir(state_code), STATE_CONDITIONS_FILE_NAME)
    if not os.path.exists(yaml_path):
        return {}
    return parse_state_conditions(
        yaml_path, sentence_class=state_inputs.sentence_class(state_code)
    )


def load_law_regime(*, state_code: StateCode, regime_name: str) -> LawRegime:
    """Returns the LawRegime named regime_name for state_code."""
    yaml_path = law_regime_yaml_path(state_code=state_code, regime_name=regime_name)
    if not os.path.exists(yaml_path):
        raise ValueError(
            f"No law regime [{regime_name}] defined for state [{state_code.value}]. "
            f"Expected a file at [{yaml_path}]."
        )
    regime = LawRegime.from_yaml(
        yaml_path,
        sentence_class=state_inputs.sentence_class(state_code),
        shared_conditions=load_state_conditions(state_code),
    )
    if regime.state_code is not state_code:
        raise ValueError(
            f"Law regime file [{yaml_path}] has state_code "
            f"[{regime.state_code.value}], but it is in the directory for state "
            f"[{state_code.value}]. Make the state_code in the file match its "
            f"directory."
        )
    return regime
