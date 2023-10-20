# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_AR. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ar_custom_enum_parsers.<function name>
"""

from typing import Optional

from recidiviz.common.constants.state.state_person import StateEthnicity


def parse_ethnic_group(
    raw_text: str,
) -> Optional[StateEthnicity]:
    if raw_text in [
        "03",  # Cuban
        "09",  # Hispanic or Latino
        "10",  # South American
        "11",  # Central America
        "23",  # Mexican American
        "24",  # Mexican National
        "27",  # Puerto Rican
        "33",  # Spain (note: by most definitions, Hispanic but not Latino)
        "35",  # Peru
        "36",  # Panama
        "37",  # Boliva
        "40",  # Mariel-Cuban
    ]:
        return StateEthnicity.HISPANIC

    if raw_text in ["98", "99"]:
        return StateEthnicity.EXTERNAL_UNKNOWN

    return StateEthnicity.NOT_HISPANIC if raw_text else None
