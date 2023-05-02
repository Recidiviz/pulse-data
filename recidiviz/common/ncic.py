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
"""
Common utility functions used to access NCIC codes.

NCIC codes are national standard codes for offenses as tracked in the central
National Crime Information Center database. They are 4-digit codes that are
organized into blocks by the hundreds, such that similar offenses are colocated.
Different justice agencies around the nation may have their own offense codes,
statute numbers, and so forth, but many of them will also have a reference to
the central NCIC codes.

The `data_sets/ncic.csv` file contains a list of NCIC codes and their attributes as
constructed in `ncic_to_nibrs_to_uccs` using the following query:

SELECT DISTINCT
  FORMAT("%04d", ncic_code) AS ncic_code,
  FORMAT("%02d", ncic_category_code) AS ncic_category_code,
  UPPER(ncic_category) AS category,
  UPPER(ncic_description) AS description,
  IF(is_drug = 1, "Y", "N") AS is_drug,
  IF(is_violent = "1", "Y", "N") AS is_violent,
FROM
  `recidiviz-123.static_reference_tables.ncic_to_nibrs_to_uccs`
WHERE
  ncic_code IS NOT NULL
ORDER BY 1

No judgments are made as to the contents of the list and the descriptions of
certain offenses. This is a reference tool.
"""

import csv
from typing import Dict, List, Optional

import attr

from recidiviz.common.attr_mixins import DefaultableAttr
from recidiviz.common.local_file_paths import filepath_relative_to_caller


@attr.s(frozen=True)
class NcicCode(DefaultableAttr):
    ncic_code: str = attr.ib()
    category_code: str = attr.ib()
    category: str = attr.ib()
    description: str = attr.ib()
    is_drug: bool = attr.ib()
    is_violent: bool = attr.ib()


_NCIC_FILEPATH: str = filepath_relative_to_caller(
    "ncic.csv", relative_subdir="data_sets"
)
_NCIC: Dict[str, NcicCode] = {}


def _get_NCIC() -> Dict[str, NcicCode]:
    if not _NCIC:
        with open(_NCIC_FILEPATH, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                code = row["ncic_code"]
                _NCIC[code] = NcicCode(
                    ncic_code=code,
                    category_code=row["category_code"].upper(),
                    category=row["category"].upper(),
                    description=row["description"].upper(),
                    is_drug=row["is_drug"] == "Y",
                    is_violent=row["is_violent"] == "Y",
                )

    return _NCIC


def get_all_codes() -> List[NcicCode]:
    ncic = _get_NCIC()
    return list(ncic.values())


def get(ncic_code: str) -> Optional[NcicCode]:
    ncic = _get_NCIC()
    return ncic.get(ncic_code, None)


def get_category_code(ncic_code: str) -> Optional[str]:
    code = get(ncic_code)
    if code:
        return code.category_code
    return None


def get_category(ncic_code: str) -> Optional[str]:
    code = get(ncic_code)
    if code:
        return code.category
    return None


def get_description(ncic_code: str) -> Optional[str]:
    code = get(ncic_code)
    if code:
        return code.description
    return None


def get_is_drug(ncic_code: str) -> Optional[bool]:
    code = get(ncic_code)
    if code:
        return code.is_drug
    return None


def get_is_violent(ncic_code: str) -> Optional[bool]:
    code = get(ncic_code)
    if code:
        return code.is_violent
    return None
