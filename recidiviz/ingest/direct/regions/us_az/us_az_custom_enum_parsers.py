# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_AZ. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_az_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_person import StateEthnicity


def parse_ethnicity(
    ## Some ethnicities are only included as a race, so we pipe the race field into this
    ## parser to set ethnicities appropriately.
    raw_text: str,
) -> Optional[StateEthnicity]:
    hispanic_options = [
        "Mexican American",
        "Mexican National",
        "Cuban",
        "Puerto Rican",
        "Peru",
        "Spain",
        "Panama",
        "Boliva",
    ]
    if raw_text:
        ethnicity = raw_text.split("##")[0]
        race = raw_text.split("##")[1]
        if ethnicity in hispanic_options or race in hispanic_options:
            return StateEthnicity.HISPANIC
        return StateEthnicity.NOT_HISPANIC
    return StateEthnicity.INTERNAL_UNKNOWN
