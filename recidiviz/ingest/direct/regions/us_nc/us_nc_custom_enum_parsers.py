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
"""Custom enum parsers functions for US_NC. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_nc_custom_enum_parsers.<function name>
"""

from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
)


def parse_student_employment_status(
    raw_text: str,  # pylint: disable=unused-argument
) -> StateEmploymentPeriodEmploymentStatus:
    """Parses any non-null ed_pgm value to STUDENT employment status.

    The raw_text will contain the education program name, and we map any value
    to STUDENT status since having an ed_pgm value indicates the person is a student.
    """
    return StateEmploymentPeriodEmploymentStatus.STUDENT
