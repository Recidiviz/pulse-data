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

"""Constants used by StateCourtCase."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


# TODO(#8905): Change superclass to Enum, remove EntityEnumMeta metaclass, and delete
#  _get_default_map() once all state ingest views have been migrated to v2 mappings.
@unique
class StateCourtType(EntityEnum, metaclass=EntityEnumMeta):
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info

    @staticmethod
    def _get_default_map() -> Dict[str, "StateCourtType"]:
        return _STATE_COURT_TYPE_MAP


# TODO(#8905): Change superclass to Enum, remove EntityEnumMeta metaclass, and delete
#  _get_default_map() once all state ingest views have been migrated to v2 mappings.
@unique
class StateCourtCaseStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info

    @staticmethod
    def _get_default_map() -> Dict[str, "StateCourtCaseStatus"]:
        return _STATE_COURT_CASE_STATUS_MAP


_STATE_COURT_TYPE_MAP: Dict[str, StateCourtType] = {
    "PRESENT WITHOUT INFO": StateCourtType.PRESENT_WITHOUT_INFO,
}

_STATE_COURT_CASE_STATUS_MAP: Dict[str, StateCourtCaseStatus] = {
    "EXTERNAL UNKNOWN": StateCourtCaseStatus.EXTERNAL_UNKNOWN,
    "PRESENT WITHOUT INFO": StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
}
