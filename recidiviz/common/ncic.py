# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""
Common utility functions used to access NCIC codes.

NCIC codes are national standard codes for offenses as tracked in the central
National Crime Information Center database. They are 4-digit codes that are
organized into blocks by the hundreds, such that similar offenses are colocated.
Different justice agencies around the nation may have their own offense codes,
statute numbers, and so forth, but many of them will also have a reference to
the central NCIC codes.

The `data_sets/ncic.csv` file contains a list of NCIC codes, their descriptions
(as provided by North Dakota DOCR), and whether or not these are considered to
be violent offenses.

No judgements are made as to the contents of the list and the descriptions of
certain offenses. This is a reference tool.
"""

import csv
from typing import Dict, Optional, List

import attr

from recidiviz.common.attr_mixins import DefaultableAttr
from recidiviz.tests.ingest.fixtures import as_filepath


@attr.s(frozen=True)
class NcicCode(DefaultableAttr):
    ncic_code: str = attr.ib()
    description: str = attr.ib()
    is_violent: bool = attr.ib()


def _initialize_codes() -> Dict[str, NcicCode]:
    ncic_codes = {}
    with open(_NCIC_FILEPATH, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            code = row["ncic_code"]
            ncic_codes[code] = NcicCode(
                ncic_code=code,
                description=row["description"].upper(),
                is_violent=row["is_violent"] == "Y",
            )

    return ncic_codes


_NCIC_FILEPATH: str = as_filepath("ncic.csv", subdir="data_sets")
_NCIC: Dict[str, NcicCode] = _initialize_codes()


def get_all_codes() -> List[NcicCode]:
    return list(_NCIC.values())


def get(ncic_code: str) -> Optional[NcicCode]:
    return _NCIC.get(ncic_code, None)


def get_description(ncic_code: str) -> Optional[str]:
    code = get(ncic_code)
    if code:
        return code.description
    return None


def get_is_violent(ncic_code: str) -> Optional[bool]:
    code = get(ncic_code)
    if code:
        return code.is_violent
    return None
