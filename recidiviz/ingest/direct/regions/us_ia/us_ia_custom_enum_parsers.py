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
"""Custom enum parsers functions for US_IA. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ia_custom_enum_parsers.<function name>
"""

from recidiviz.common.constants.state.state_sentence import StateSentenceType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)


def map_to_probation_but_retain_raw_text(
    raw_text: str,
) -> StateSentenceType:
    """Maps sentence type to PROBATION for sentences we've already identified as being suspended with probation;
    used instead of literal enum so that raw text of the county jail name can be preserved"""

    if raw_text:
        return StateSentenceType.PROBATION

    raise ValueError("This parser should never be called on missing raw text.")


def map_to_residential_but_retain_raw_text(
    raw_text: str,
) -> StateSupervisionLevel:
    """Maps supervision level to RESIDENTIAL_PROGRAM for supervision periods with a residential services location type;
    used instead of literal enum so that raw text of the original supervision level can be preserved"""

    if raw_text:
        return StateSupervisionLevel.RESIDENTIAL_PROGRAM

    raise ValueError("This parser should never be called on missing raw text.")
