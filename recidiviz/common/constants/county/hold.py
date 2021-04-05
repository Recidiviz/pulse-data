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

"""Constants related to a hold entity."""
from typing import Dict

from recidiviz.common.constants.county import (
    enum_canonical_strings as county_enum_strings,
)
from recidiviz.common.constants import enum_canonical_strings as enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class HoldStatus(EntityEnum, metaclass=EntityEnumMeta):
    ACTIVE = county_enum_strings.hold_status_active
    INACTIVE = county_enum_strings.hold_status_inactive
    INFERRED_DROPPED = county_enum_strings.hold_status_inferred_dropped
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    REMOVED_WITHOUT_INFO = enum_strings.removed_without_info

    @staticmethod
    def _get_default_map() -> Dict[str, "HoldStatus"]:
        return _HOLD_STATUS_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_HOLD_STATUS_MAP = {"ACTIVE": HoldStatus.ACTIVE, "INACTIVE": HoldStatus.INACTIVE}
