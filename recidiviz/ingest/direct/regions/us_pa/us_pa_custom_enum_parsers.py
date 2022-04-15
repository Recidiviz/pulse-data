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
"""Custom enum parsers functions for US_PA. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_pa_custom_enum_parsers.<function name>
"""

from recidiviz.common.constants.state.state_person import StateResidencyStatus


def residency_status_from_address(raw_text: str) -> StateResidencyStatus:
    normalized_address = raw_text.upper()
    no_stable_housing_indicators = ["HOMELESS", "TRANSIENT"]
    for indicator in no_stable_housing_indicators:
        if indicator in normalized_address:
            # TODO(#9301): Use the term NO_STABLE_HOUSING in the schema instead of
            #  HOMELESS / TRANSIENT.
            return StateResidencyStatus.HOMELESS

    return StateResidencyStatus.PERMANENT
