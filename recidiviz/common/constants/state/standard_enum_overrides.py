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
"""Function for accessing a set of overrides that are used in state direct ingest
controllers.
"""

from typing import cast

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_person import (
    STATE_ETHNICITY_MAP,
    StateEthnicity,
    StateRace,
)


# TODO(#8905): Delete once all state ingest views have been migrated to v2 mappings.
def legacy_mappings_standard_enum_overrides() -> EnumOverrides:
    """
    Returns a dict that contains all string to enum mappings that are region specific. These overrides have a higher
    precedence than the global mappings in ingest/constants.

    Note: Before overriding this method, consider directly adding each mapping directly into the respective global
    mappings instead.
    """
    overrides_builder = EnumOverrides.Builder()
    for ethnicity_string, ethnicity in STATE_ETHNICITY_MAP.items():
        # mypy is unable to correctly type the EntityEnums in constants.person. See
        # https://github.com/python/mypy/issues/3327
        ethnicity_enum = cast(StateEthnicity, ethnicity)
        if ethnicity_enum is StateEthnicity.HISPANIC:
            overrides_builder.add(ethnicity_string, ethnicity_enum, StateRace)

    return overrides_builder.build()
