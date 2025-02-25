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
"""Custom parser functions for US_NC. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_nd_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""


def get_county_from_facility(raw_text: str) -> str:
    if "COUNTY" in raw_text:
        return raw_text[:-7]
    return ""
