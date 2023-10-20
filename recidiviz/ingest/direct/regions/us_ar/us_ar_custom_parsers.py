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
"""Custom parser functions for US_AR. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_ar_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""

from typing import Optional


def parse_address_pieces(
    stnum: str,
    stname: str,
    sttype: str,
    suite: str,
    apt: str,
    po: str,
    city: str,
    st: str,
    zipcode: str,
) -> Optional[str]:
    """Takes in a MO raw county code and returns
    a Recidiviz-normalized county code in US_XX_YYYYY format.
    """
    if not any([stnum, stname, suite, apt, po, city, st, zipcode]):
        return None
    constructed_address = ""

    if stnum and stname:
        constructed_address += stnum + " " + stname
        if sttype:
            constructed_address += " " + sttype
    if suite:
        constructed_address += ", SUITE " + suite
    if apt:
        constructed_address += ", APT. " + apt
    if po:
        constructed_address += ", PO BOX " + po
    if city and st:
        constructed_address += ", " + city + " " + st
    if zipcode:
        constructed_address += " " + zipcode

    return constructed_address.upper()
