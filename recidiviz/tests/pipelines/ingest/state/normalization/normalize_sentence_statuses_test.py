# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
This module tests code to normalize StateSentenceStatusSnapshot entities.
"""

from datetime import datetime

from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentence,
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.ingest.state.normalization.sentencing.normalize_sentence_statuses import (
    split_snapshot_by_status,
)


def test_split_snapshot_by_status() -> None:
    """
    Tests we correctly split a single snapshot.

    This example snapshot starts and ends from 2024-01-01 to 2025-01-01.
    It is the child sentence and we initially believe that sentence is being served during that entire time period.
    However, the parent sentence is actually completed on 2024-06-01 (in the middle of that serving period),
    so we are updating the child sentence status to be split into two sentence statuses.
    We assume that because these are consecutive sentences, that the child sentence is actually
    IMPOSED_PENDING_SERVING from 2024-01-01 to 2024-06-01 (period before the parent sentence completed),
    and then is SERVING from 2024-06-01 to 2025-01-01 (period after the parent sentence is completed).
    """
    parent_sentence = NormalizedStateSentence(
        state_code="US_XX",
        external_id="parent-sentence-id",
        imposed_date=datetime(2024, 1, 1).date(),
        sentence_id=-1,
        sentence_inferred_group_id=-1,
        sentence_imposed_group_id=-1,
        sentence_type=StateSentenceType.STATE_PRISON,
        sentencing_authority=StateSentencingAuthority.STATE,
    )

    snapshot = NormalizedStateSentenceStatusSnapshot(
        state_code="US_XX",
        status=StateSentenceStatus.SERVING,
        status_update_datetime=datetime(2024, 1, 1),
        status_end_datetime=datetime(2025, 1, 1),
        status_raw_text="some obfuscated code we parsed",
        sentence=parent_sentence,
    )

    first, second = split_snapshot_by_status(
        original_snapshot=snapshot,
        split_dt=datetime(2024, 6, 1),
        first_status=StateSentenceStatus.IMPOSED_PENDING_SERVING,
        second_status=StateSentenceStatus.SERVING,
        second_status_raw_text_override="CREATED IN NORMALIZATION",
    )

    assert first.status == StateSentenceStatus.IMPOSED_PENDING_SERVING
    assert first.status_update_datetime == datetime(2024, 1, 1)
    assert first.status_end_datetime == datetime(2024, 6, 1)
    assert first.status_raw_text == "some obfuscated code we parsed"

    assert second.status == StateSentenceStatus.SERVING
    assert second.status_update_datetime == datetime(2024, 6, 1)
    assert second.status_end_datetime == datetime(2025, 1, 1)
    assert second.status_raw_text == "CREATED IN NORMALIZATION"
