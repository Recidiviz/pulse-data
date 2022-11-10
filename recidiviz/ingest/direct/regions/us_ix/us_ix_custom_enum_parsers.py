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
"""Custom enum parsers functions for US_IX. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ix_custom_enum_parsers.<function name>
"""
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactMethod,
)


def contact_method_from_contact_fields(raw_text: str) -> StateSupervisionContactMethod:
    location_text, type_text = raw_text.split("##")
    if location_text == "TELEPHONE":
        return StateSupervisionContactMethod.TELEPHONE
    if location_text in ("MAIL", "EMAIL", "FAX", "WBOR"):
        return StateSupervisionContactMethod.WRITTEN_MESSAGE
    if type_text == "VIRTUAL":
        return StateSupervisionContactMethod.VIRTUAL
    if type_text == "WRITTEN CORRESPONDENCE":
        return StateSupervisionContactMethod.WRITTEN_MESSAGE
    if (
        type_text not in ("COLLATERAL", "MENTAL HEALTH COLLATERAL")
        and location_text != "NONE"
    ):
        return StateSupervisionContactMethod.IN_PERSON
    if type_text in ("NEGATIVE CONTACT", "447", "OFFICE"):
        return StateSupervisionContactMethod.IN_PERSON
    return StateSupervisionContactMethod.INTERNAL_UNKNOWN
