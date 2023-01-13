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

"""Custom parser functions for US_IX. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_me_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""

from typing import Optional

from recidiviz.common.str_field_utils import parse_datetime


def parse_duration_from_two_dates(
    start_date_str: str, end_date_str: str
) -> Optional[str]:
    try:
        start_dt = parse_datetime(start_date_str)
        end_dt = parse_datetime(end_date_str)

        if start_dt and end_dt and end_dt.year != 9999:
            return str((end_dt - start_dt).days)
    except ValueError:
        return None
    return None


def parse_supervision_violation_is_violent(new_crime_types: str) -> bool:

    for new_crime_type in new_crime_types.split(","):
        if (
            "VIOLENT" in new_crime_type.upper()
            and "NON-VIOLENT" not in new_crime_type.upper()
        ):
            return True
    return False


def parse_supervision_violation_is_sex_offense(new_crime_types: str) -> bool:

    for new_crime_type in new_crime_types.split(","):
        if "SEX" in new_crime_type.upper():
            return True
    return False


def parse_is_life_from_date(proj_completion_date: str) -> bool:
    return proj_completion_date[:4] == "9999"
