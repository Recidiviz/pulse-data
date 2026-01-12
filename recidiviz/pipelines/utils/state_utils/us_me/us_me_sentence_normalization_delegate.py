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
"""Contains US_ME implementation of the StateSpecificSentenceNormalizationDelegate."""
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)

NORMALIZED_SENTENCE_TYPES = [
    StateSentenceType.COUNTY_JAIL,
    StateSentenceType.STATE_PRISON,
]


class UsMeSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_ME implementation of the StateSpecificSentenceNormalizationDelegate."""

    def get_sentence_type_override(
        self,
        sentence_type: StateSentenceType,
        normalized_snapshots: list[NormalizedStateSentenceStatusSnapshot],
    ) -> StateSentenceType | None:
        """If a COUNTY_JAIL or STATE_PRISON sentence gets revoked (happens, but very rarely),
        we should convert it to a SPLIT sentence because we can assume that the revocation
        occurred while the resident was on supervision for that sentence."""

        if (sentence_type in NORMALIZED_SENTENCE_TYPES) and any(
            s.status == StateSentenceStatus.REVOKED for s in normalized_snapshots
        ):
            return StateSentenceType.SPLIT

        return None
