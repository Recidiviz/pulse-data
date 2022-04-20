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
"""Function for accessing a set of overrides that are used in county scrapers.
"""

from typing import Optional, cast

from recidiviz.common.constants.county.bond import BondStatus, BondType
from recidiviz.common.constants.county.charge import ChargeClass, ChargeStatus
from recidiviz.common.constants.county.person_characteristics import (
    ETHNICITY_MAP,
    Ethnicity,
    Race,
)
from recidiviz.common.constants.enum_overrides import EnumOverrides


# TODO(#2056): Move this logic into the converters themselves.
def get_standard_enum_overrides() -> EnumOverrides:
    """
    Returns a dict that contains all string to enum mappings that are region specific. These overrides have a higher
    precedence than the global mappings in ingest/constants.

    Note: Before overriding this method, consider directly adding each mapping directly into the respective global
    mappings instead.
    """
    overrides_builder = EnumOverrides.Builder()
    for ethnicity_string, ethnicity in ETHNICITY_MAP.items():
        # mypy is unable to correctly type the EntityEnums in constants.person. See
        # https://github.com/python/mypy/issues/3327
        ethnicity_enum = cast(Ethnicity, ethnicity)
        if ethnicity_enum is Ethnicity.HISPANIC:
            overrides_builder.add(ethnicity_string, ethnicity_enum, Race)

    overrides_builder.add("OUT ON BOND", BondStatus.POSTED, BondType)
    overrides_builder.add_mapper_fn(_felony_mapper, ChargeClass, ChargeStatus)

    return overrides_builder.build()


def _felony_mapper(status: str) -> Optional[ChargeClass]:
    if "FELONY" in status or "MURDER" in status:
        return ChargeClass.FELONY
    return None
