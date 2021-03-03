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

"""Consatnts related to fine sentences."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


@unique
class StateFineStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    PAID = state_enum_strings.state_fine_status_paid
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    UNPAID = state_enum_strings.state_fine_status_unpaid

    @staticmethod
    def _get_default_map():
        return _STATE_FINE_STATUS_MAP


_STATE_FINE_STATUS_MAP: Dict[str, StateFineStatus] = {
    "EXTERNAL UNKNOWN": StateFineStatus.EXTERNAL_UNKNOWN,
    "PAID": StateFineStatus.PAID,
    "PRESENT WITHOUT INFO": StateFineStatus.PRESENT_WITHOUT_INFO,
    "UNPAID": StateFineStatus.UNPAID,
}
