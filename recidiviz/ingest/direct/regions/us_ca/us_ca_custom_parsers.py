# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Custom parsers functions for US_CA. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_ca_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""


from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
)
from recidiviz.ingest.direct.regions.us_ca.us_ca_custom_enum_parsers import (
    parse_employment_status,
)


def is_employed(employer_name: str) -> bool:
    parsed_employment_status = parse_employment_status(employer_name)

    if parsed_employment_status in (
        StateEmploymentPeriodEmploymentStatus.EMPLOYED_UNKNOWN_AMOUNT,
        StateEmploymentPeriodEmploymentStatus.EMPLOYED_FULL_TIME,
        StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME,
    ):
        return True

    return False
