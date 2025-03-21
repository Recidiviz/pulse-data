# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_IA. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_mi_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""
from typing import Optional

from recidiviz.common.str_field_utils import safe_parse_days_from_duration_pieces


def max_and_min_lengths_days(
    years_str: str,
    months_str: str,
    days_str: str,
) -> Optional[str]:
    """Returns the duration in days from days, months, and years"""
    result = safe_parse_days_from_duration_pieces(
        years_str=years_str, months_str=months_str, days_str=days_str
    )
    if result:
        if result == 0:
            return None
        return str(result)
    return None
