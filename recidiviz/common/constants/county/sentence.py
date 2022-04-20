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

"""Constants related to a sentence entity."""
from typing import Dict

from recidiviz.common.constants.county import (
    enum_canonical_strings as county_enum_strings,
)
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class SentenceStatus(EntityEnum, metaclass=EntityEnumMeta):
    COMMUTED = county_enum_strings.sentence_status_commuted
    COMPLETED = county_enum_strings.sentence_status_completed
    SERVING = county_enum_strings.sentence_status_serving
    PRESENT_WITHOUT_INFO = county_enum_strings.present_without_info
    REMOVED_WITHOUT_INFO = county_enum_strings.removed_without_info

    @staticmethod
    def _get_default_map() -> Dict[str, "SentenceStatus"]:
        return _SENTENCE_STATUS_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_SENTENCE_STATUS_MAP = {
    "COMMUTED": SentenceStatus.COMMUTED,
    "COMPLETED": SentenceStatus.COMPLETED,
    "SERVING": SentenceStatus.SERVING,
}
