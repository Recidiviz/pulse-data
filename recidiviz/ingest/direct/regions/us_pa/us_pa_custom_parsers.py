# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_PA. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_pa_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""


from typing import Dict, List, Optional

from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.legacy_ingest_mappings.direct_ingest_controller_utils import (
    invert_str_to_str_mappings,
)

# A static reference cache for converting violation type codes into other relevant
# information, such as violation conditions. These mappings are taken from PBPP's
# Violation Sanctioning Grid (VSG).
_VIOLATION_CONDITIONS: Dict[str, List[str]] = {
    "1": ["H06", "M04"],
    "2": ["H01", "H09"],
    "3": ["M02", "M19", "L07", "M01"],
    "4": ["M13"],
    "5": ["L08", "M03", "H12", "H10", "H11", "H08"],
    "6": ["L06"],
    "7": [
        "L01",
        "L03",
        "L04",
        "L05",
        "M05",
        "M06",
        "M07",
        "M08",
        "M09",
        "M10",
        "M11",
        "M12",
        "L02",
        "M14",
        "H03",
        "M15",
        "M16",
        "M17",
        "M18",
        "M20",
        "H02",
        "H05",
        "H07",
        "H04",
    ],
}

_VIOLATION_CONDITIONS_BY_CODE: Dict[str, str] = invert_str_to_str_mappings(
    _VIOLATION_CONDITIONS
)


def violated_condition_from_violation_code(violation_code: str) -> str:
    """Returns one of the canonical PBPP violation conditions, of which there are 7
    (numbered 1-7), based on the given violation type.

    That is, each violation type is mapped to one of 7 conditions, as described in the
    Violation Sanction Grid.
    """
    condition = _VIOLATION_CONDITIONS_BY_CODE.get(violation_code.upper(), None)

    if not condition:
        raise ValueError(
            f"Found new violation type code not in condition reference cache: "
            f"[{violation_code}]",
        )
    return condition


def max_and_min_lengths_days_from_court_sentence_duration(
    years_str: str,
    months_str: str,
    days_str: str,
    start_date_str: str,
) -> Optional[str]:
    """Returns the duration in days from a start date with given number of years, months
    and days."""
    result = parse_days_from_duration_pieces(
        years_str, months_str, days_str, start_date_str
    )
    if result == 0:
        return None
    return str(result)
