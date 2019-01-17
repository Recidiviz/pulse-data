# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.common.constants.mappable_enum import MappableEnum


class SentenceStatus(MappableEnum):
    COMMUTED = enum_strings.sentence_status_commuted
    COMPLETED = enum_strings.sentence_status_completed
    SERVING = enum_strings.sentence_status_serving
    UNKNOWN_FOUND_IN_SOURCE = enum_strings.unknown_found_in_source
    UNKNOWN_REMOVED_FROM_SOURCE = enum_strings.unknown_removed_from_source

    @staticmethod
    def _get_default_map():
        return _SENTENCE_STATUS_MAP


_SENTENCE_STATUS_MAP = {
    'COMMUTED': SentenceStatus.COMMUTED,
    'COMPLETED': SentenceStatus.COMPLETED,
    'SERVING': SentenceStatus.SERVING,
}
