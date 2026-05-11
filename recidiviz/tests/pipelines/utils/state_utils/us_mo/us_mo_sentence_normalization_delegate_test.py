#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests us_mo_sentence_normalization_delegate.py."""
import unittest
from datetime import date, datetime

from recidiviz.common.constants.state.state_charge import (
    StateChargeV2ClassificationType,
    StateChargeV2Status,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
    NormalizedStateSentenceLength,
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_sentence_normalization_delegate import (
    UsMoSentenceNormalizationDelegate,
)


class TestUsMoSentenceNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsMoSentenceNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsMoSentenceNormalizationDelegate()

    def test_charge_severity_ranking_prioritizes_classification_type(self) -> None:
        sentence_1 = NormalizedStateSentence(
            sentence_id=1,
            state_code=StateCode.US_MO.value,
            external_id="s1",
            sentence_group_external_id="g1",
            sentence_inferred_group_id=1,
            sentence_imposed_group_id=1,
            imposed_date=date(2020, 1, 1),
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_MO.value,
                    status_update_datetime=datetime(2020, 1, 1),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
            ],
            charges=[
                NormalizedStateChargeV2(
                    charge_v2_id=1,
                    state_code=StateCode.US_MO.value,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                    external_id="s1",
                    county_code="US_MO_JACKSON",
                    description="THEFT",
                    is_violent=False,
                    classification_type=StateChargeV2ClassificationType.FELONY,
                    classification_subtype="B",
                )
            ],
            sentence_lengths=[
                NormalizedStateSentenceLength(
                    state_code=StateCode.US_MO.value,
                    sentence_length_id=1,
                    length_update_datetime=datetime(2020, 1, 1),
                    sentence_length_days_max=100,
                )
            ],
        )
        sentence_2 = NormalizedStateSentence(
            sentence_id=2,
            state_code=StateCode.US_MO.value,
            external_id="s2",
            sentence_group_external_id="g1",
            sentence_inferred_group_id=1,
            sentence_imposed_group_id=1,
            imposed_date=date(2020, 1, 1),
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_MO.value,
                    status_update_datetime=datetime(2020, 1, 1),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
            ],
            charges=[
                NormalizedStateChargeV2(
                    charge_v2_id=2,
                    state_code=StateCode.US_MO.value,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                    external_id="s2",
                    county_code="US_MO_JACKSON",
                    description="BURGLARY",
                    is_violent=False,
                    classification_type=StateChargeV2ClassificationType.FELONY,
                    classification_subtype="C",
                )
            ],
            sentence_lengths=[
                NormalizedStateSentenceLength(
                    state_code=StateCode.US_MO.value,
                    sentence_length_id=1,
                    length_update_datetime=datetime(2020, 1, 1),
                    sentence_length_days_max=200,
                )
            ],
        )
        sentence_3 = NormalizedStateSentence(
            sentence_id=3,
            state_code=StateCode.US_MO.value,
            external_id="s3",
            sentence_group_external_id="g1",
            sentence_inferred_group_id=1,
            sentence_imposed_group_id=1,
            imposed_date=date(2020, 1, 1),
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_MO.value,
                    status_update_datetime=datetime(2020, 1, 1),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
            ],
            charges=[
                NormalizedStateChargeV2(
                    charge_v2_id=3,
                    state_code=StateCode.US_MO.value,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                    external_id="s3",
                    county_code="US_MO_JACKSON",
                    description="TRESPASSING",
                    is_violent=False,
                    classification_type=StateChargeV2ClassificationType.MISDEMEANOR,
                    classification_subtype="A",
                )
            ],
            sentence_lengths=[
                NormalizedStateSentenceLength(
                    state_code=StateCode.US_MO.value,
                    sentence_length_id=1,
                    length_update_datetime=datetime(2020, 1, 1),
                    sentence_length_days_max=200,
                )
            ],
        )
        self.assertEqual(
            self.delegate.get_most_severe_charge(
                [sentence_1, sentence_2, sentence_3]
            ).charge_v2_id,
            1,
        )

    def test_charge_severity_ranking_prioritizes_unclassified_charges_correctly(
        self,
    ) -> None:
        sentence_1 = NormalizedStateSentence(
            sentence_id=1,
            state_code=StateCode.US_MO.value,
            external_id="s1",
            sentence_group_external_id="g1",
            sentence_inferred_group_id=1,
            sentence_imposed_group_id=1,
            imposed_date=date(2020, 1, 1),
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_MO.value,
                    status_update_datetime=datetime(2020, 1, 1),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
            ],
            charges=[
                NormalizedStateChargeV2(
                    charge_v2_id=1,
                    state_code=StateCode.US_MO.value,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                    external_id="s1",
                    county_code="US_MO_JACKSON",
                    description="THEFT",
                    is_violent=False,
                    classification_type=StateChargeV2ClassificationType.FELONY,
                    classification_subtype="B",
                )
            ],
            sentence_lengths=[
                NormalizedStateSentenceLength(
                    state_code=StateCode.US_MO.value,
                    sentence_length_id=1,
                    length_update_datetime=datetime(2020, 1, 1),
                    sentence_length_days_max=100,
                )
            ],
        )
        sentence_2 = NormalizedStateSentence(
            sentence_id=2,
            state_code=StateCode.US_MO.value,
            external_id="s2",
            sentence_group_external_id="g1",
            sentence_inferred_group_id=1,
            sentence_imposed_group_id=1,
            imposed_date=date(2020, 1, 1),
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_MO.value,
                    status_update_datetime=datetime(2020, 1, 1),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
            ],
            charges=[
                NormalizedStateChargeV2(
                    charge_v2_id=2,
                    state_code=StateCode.US_MO.value,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                    external_id="s2",
                    county_code="US_MO_JACKSON",
                    description="BURGLARY",
                    is_violent=False,
                    classification_type=StateChargeV2ClassificationType.FELONY,
                    classification_subtype="U",
                )
            ],
            sentence_lengths=[
                NormalizedStateSentenceLength(
                    state_code=StateCode.US_MO.value,
                    sentence_length_id=1,
                    length_update_datetime=datetime(2020, 1, 1),
                    sentence_length_days_max=200,
                )
            ],
        )
        self.assertEqual(
            self.delegate.get_most_severe_charge([sentence_1, sentence_2]).charge_v2_id,
            2,
        )
        sentence_3 = NormalizedStateSentence(
            sentence_id=3,
            state_code=StateCode.US_MO.value,
            external_id="s3",
            sentence_group_external_id="g1",
            sentence_inferred_group_id=1,
            sentence_imposed_group_id=1,
            imposed_date=date(2020, 1, 1),
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=StateCode.US_MO.value,
                    status_update_datetime=datetime(2020, 1, 1),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
            ],
            charges=[
                NormalizedStateChargeV2(
                    charge_v2_id=3,
                    state_code=StateCode.US_MO.value,
                    status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
                    external_id="s3",
                    county_code="US_MO_JACKSON",
                    description="MURDER",
                    is_violent=True,
                    classification_type=StateChargeV2ClassificationType.FELONY,
                    classification_subtype="A",
                )
            ],
            sentence_lengths=[
                NormalizedStateSentenceLength(
                    state_code=StateCode.US_MO.value,
                    sentence_length_id=1,
                    length_update_datetime=datetime(2020, 1, 1),
                    sentence_length_days_max=200,
                )
            ],
        )
        self.assertEqual(
            self.delegate.get_most_severe_charge(
                [sentence_1, sentence_2, sentence_3]
            ).charge_v2_id,
            3,
        )
