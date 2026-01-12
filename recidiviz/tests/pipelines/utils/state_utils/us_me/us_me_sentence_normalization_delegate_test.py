#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests us_me_supervision_period_normalization_delegate.py."""
import unittest
from datetime import datetime

from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_sentence_normalization_delegate import (
    UsMeSentenceNormalizationDelegate,
)

_STATE_CODE = StateCode.US_ME.value


class TestUsMeSentenceNormalizationDelegate(unittest.TestCase):
    """Tests for UsMeSentenceNormalizationDelegate."""

    def test_sentence_type_override_for_revocations(self) -> None:
        """Tests that the sentence type override is applied correctly for revoked sentences."""
        serving_snapshot = NormalizedStateSentenceStatusSnapshot(
            status=StateSentenceStatus.SERVING,
            state_code=_STATE_CODE,
            status_update_datetime=datetime(2025, 1, 1),
            sentence_status_snapshot_id=0,
        )
        revoked_snapshot = NormalizedStateSentenceStatusSnapshot(
            status=StateSentenceStatus.REVOKED,
            state_code=_STATE_CODE,
            status_update_datetime=datetime(2025, 2, 1),
            sentence_status_snapshot_id=1,
        )
        self.assertEqual(
            StateSentenceType.SPLIT,
            UsMeSentenceNormalizationDelegate().get_sentence_type_override(
                StateSentenceType.COUNTY_JAIL, [serving_snapshot, revoked_snapshot]
            ),
        )
