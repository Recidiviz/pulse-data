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
"""Function for accessing a set of overrides that are used in all
data parsing, i.e. both scrapers and direct ingest controllers.
"""

from typing import cast, Optional

from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.charge import ChargeClass
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import ETHNICITY_MAP, \
    Ethnicity, Race


# TODO(#2056): Move this logic into the converters themselves.
def get_standard_enum_overrides() -> EnumOverrides:
    """
    Returns a dict that contains all string to enum mappings that are region specific. These overrides have a higher
    precedence than the global mappings in ingest/constants.

    Note: Before overriding this method, consider directly adding each mapping directly into the respective global
    mappings instead.
    """
    overrides_builder = EnumOverrides.Builder()
    for ethnicity_string in ETHNICITY_MAP:
        # mypy is unable to correctly type the EntityEnums in constants.person. See
        # https://github.com/python/mypy/issues/3327
        ethnicity_enum = cast(Ethnicity, ETHNICITY_MAP[ethnicity_string])
        if ethnicity_enum is Ethnicity.HISPANIC:
            overrides_builder.add(ethnicity_string, ethnicity_enum, Race)

    overrides_builder.add('OUT ON BOND', BondStatus.POSTED, BondType)
    overrides_builder.add_mapper(_felony_mapper, ChargeClass, ChargeStatus)

    return overrides_builder.build()


def _felony_mapper(status: str) -> Optional[ChargeClass]:
    if 'FELONY' in status or 'MURDER' in status:
        return ChargeClass.FELONY
    return None
