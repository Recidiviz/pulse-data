# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_MI. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_mi_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_agent import StateAgentType


def parse_agent_type(
    raw_text: str,
) -> Optional[StateAgentType]:
    """Parse agent type based on employee position raw text"""

    supervision_officer_positions = [
        "PAROLE PROBATION",
        "PAROLE/PRBTN",
        "PAROLE/PROBATION",
    ]

    # if position indicates parole/probation, then set as supervision officer
    if any(position in raw_text.upper() for position in supervision_officer_positions):
        return StateAgentType.SUPERVISION_OFFICER

    # else if not the above case but position is provided, set as internal unknown
    if raw_text:
        return StateAgentType.INTERNAL_UNKNOWN

    return None
