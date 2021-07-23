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
"""Helpers for generating shared SQL fragments for vitals queries."""
from typing import Dict, List, Optional

# TODO(#8387): Clean up query generation for vitals dash views

ENABLED_VITALS: Dict[str, List[str]] = {
    "US_ND": ["timely_contact", "timely_risk_assessment", "timely_discharge"],
    "US_ID": ["timely_contact", "timely_risk_assessment", "timely_downgrade"],
}


def make_enabled_states_filter(
    enabled_vitals: Optional[Dict[str, List[str]]] = None,
) -> str:
    """Generates a boolean statement for use in a WHEN query.
    Alows all states listed as keys in `enabled_vitals`.
    """
    if enabled_vitals is None:
        enabled_vitals = ENABLED_VITALS

    enabled_states = ", ".join(f"'{state}'" for state in enabled_vitals)

    return f"state_code in ({enabled_states})"


def make_enabled_states_filter_for_vital(
    vital: str,
    enabled_vitals: Optional[Dict[str, List[str]]] = None,
) -> str:
    """Generates a boolean statement for use in a WHEN query.
    Allows all states in `enabled_vitals` with `vital` listed.
    """
    if enabled_vitals is None:
        enabled_vitals = ENABLED_VITALS

    enabled_states = ", ".join(
        f"'{state}'" for state, vitals in enabled_vitals.items() if vital in vitals
    )

    return f"state_code in ({enabled_states})"
