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

"""Constants related to a state Sentence."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateSentenceStatus(EntityEnum, metaclass=EntityEnumMeta):
    # Commuted means that the sentence has been shortened/lessened, all the way to today, but the judgment remains
    COMMUTED = state_enum_strings.state_sentence_status_commuted
    COMPLETED = state_enum_strings.state_sentence_status_completed
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    # Pardoned means that the executive branch has removed the conviction after the fact,
    # but the judgment may remain until expunged from the record in some cases
    PARDONED = state_enum_strings.state_sentence_status_pardoned
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    # Revoked means someone was on probation and that probation sentence was revoked / turned into incarceration
    REVOKED = state_enum_strings.state_sentence_status_revoked
    SERVING = state_enum_strings.state_sentence_status_serving
    SUSPENDED = state_enum_strings.state_sentence_status_suspended
    # Vacated means that a previous judgment was voided by the judiciary - the judgment is completely undone
    VACATED = state_enum_strings.state_sentence_status_vacated

    @staticmethod
    def _get_default_map():
        return _STATE_SENTENCE_STATUS_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_STATE_SENTENCE_STATUS_MAP = {
    'COMMUTED': StateSentenceStatus.COMMUTED,
    'COMPLETED': StateSentenceStatus.COMPLETED,
    'EXTERNAL UNKNOWN': StateSentenceStatus.EXTERNAL_UNKNOWN,
    'PARDONED': StateSentenceStatus.PARDONED,
    'PRESENT WITHOUT INFO': StateSentenceStatus.PRESENT_WITHOUT_INFO,
    'REVOKED': StateSentenceStatus.REVOKED,
    'SERVING': StateSentenceStatus.SERVING,
    'SUSPENDED': StateSentenceStatus.SUSPENDED,
    'VACATED': StateSentenceStatus.VACATED,
}
