# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
Tests the creation of the NormalizedSentenceInferredGroup entity
from NormalizedStateSentence entities.
"""

import datetime
import unittest

from recidiviz.common.constants.state.state_charge import StateChargeV2Status
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import as_datetime
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateSentence,
    NormalizedStateSentenceGroup,
    NormalizedStateSentenceInferredGroup,
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.ingest.state.normalization.infer_sentence_groups import (
    get_normalized_inferred_sentence_groups,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class TestInferredSentenceGroups(unittest.TestCase):
    """
    Tests the creation of NormalizedStateSentenceInferredGroup.
    Any NormalizedStateSentenceGroup and NormalizedStateSentence
    associated with a NormalizedStateSentenceInferredGroup must
    have a set sentence_group_inferred_id.
    """

    # Assume all sentencing dates are between Jan-Jun of 2022 for this test
    JAN_01 = datetime.date(2022, 1, 1)
    FEB_01 = datetime.date(2022, 2, 1)
    MAR_01 = datetime.date(2022, 3, 1)
    MAR_15 = datetime.date(2022, 3, 15)
    APR_01 = datetime.date(2022, 4, 1)
    MAY_01 = datetime.date(2022, 5, 1)
    JUN_01 = datetime.date(2022, 6, 1)

    GROUP_A_EXTERNAL_ID = "sentence-group-a"
    GROUP_B_EXTERNAL_ID = "sentence-group-b"
    GROUP_C_EXTERNAL_ID = "sentence-group-c"

    SENTENCE_1_EXTERNAL_ID = "sentence-001"
    SENTENCE_2_EXTERNAL_ID = "sentence-002"
    SENTENCE_3_EXTERNAL_ID = "sentence-003"

    STATE_CODE = StateCode.US_XX
    DELEGATE = StateSpecificSentenceNormalizationDelegate()

    def test_from_sentences_that_only_share_state_provided_groups(self) -> None:
        """
        If two state provided sentence groups have sentences that:
            - Do NOT share an imposed_date
            - DO NOT share active SERVING time
        then they are not in the same inferred group.
        """
        group_a = NormalizedStateSentenceGroup(
            state_code=self.STATE_CODE.value,
            external_id=self.GROUP_A_EXTERNAL_ID,
            sentence_group_id=hash(self.GROUP_A_EXTERNAL_ID),
            sentence_inferred_group_id=None,
        )
        group_b = NormalizedStateSentenceGroup(
            state_code=self.STATE_CODE.value,
            external_id=self.GROUP_B_EXTERNAL_ID,
            sentence_group_id=hash(self.GROUP_A_EXTERNAL_ID),
            sentence_inferred_group_id=None,
        )

        # Sentences 1 and 2 are in group A, sentence 3 is in group B
        # Group A is from JAN-MAR, Group B begins APR 1
        sentence_1 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_1_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_1_EXTERNAL_ID),
            sentence_group_external_id=group_a.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=2,
                ),
            ],
        )
        sentence_2 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_2_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_2_EXTERNAL_ID),
            sentence_group_external_id=group_a.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=11,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=22,
                ),
            ],
        )
        sentence_3 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_3_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_3_EXTERNAL_ID),
            sentence_group_external_id=group_b.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.APR_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.APR_01),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=111,
                ),
            ],
        )
        actual_inferred_groups = get_normalized_inferred_sentence_groups(
            self.STATE_CODE,
            self.DELEGATE,
            normalized_sentences=[sentence_1, sentence_2, sentence_3],
        )
        inferred_a = NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
            self.STATE_CODE, [sentence_1.external_id, sentence_2.external_id]
        )
        inferred_b = NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
            self.STATE_CODE, [sentence_3.external_id]
        )
        assert sorted(actual_inferred_groups, key=lambda g: g.external_id) == [
            inferred_a,
            inferred_b,
        ]

    def test_groups_merge_by_imposed_date(self) -> None:
        """
        If two state provide sentence groups have sentences that
        share an imposed_date, then they are in the same inferred group.
        """
        group_a = NormalizedStateSentenceGroup(
            state_code=self.STATE_CODE.value,
            external_id=self.GROUP_A_EXTERNAL_ID,
            sentence_group_id=hash(self.GROUP_A_EXTERNAL_ID),
            sentence_inferred_group_id=None,
        )
        group_b = NormalizedStateSentenceGroup(
            state_code=self.STATE_CODE.value,
            external_id=self.GROUP_B_EXTERNAL_ID,
            sentence_group_id=hash(self.GROUP_A_EXTERNAL_ID),
            sentence_inferred_group_id=None,
        )
        # Sentences 1 and 2 are in group A, sentence 3 is in group B
        # However, sentence 1 & 3 were imposed together.
        sentence_1 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_1_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_1_EXTERNAL_ID),
            sentence_group_external_id=group_a.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=2,
                ),
            ],
        )
        sentence_2 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_2_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_2_EXTERNAL_ID),
            sentence_group_external_id=group_a.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=11,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=22,
                ),
            ],
        )
        sentence_3 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_3_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_3_EXTERNAL_ID),
            sentence_group_external_id=group_b.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=111,
                ),
            ],
        )
        actual_inferred_groups = get_normalized_inferred_sentence_groups(
            self.STATE_CODE,
            self.DELEGATE,
            normalized_sentences=[sentence_1, sentence_2, sentence_3],
        )
        inferred_group = (
            NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
                self.STATE_CODE,
                [
                    sentence_1.external_id,
                    sentence_2.external_id,
                    sentence_3.external_id,
                ],
            )
        )
        assert actual_inferred_groups == [inferred_group]
        assert (
            inferred_group.external_id == "sentence-001@#@sentence-002@#@sentence-003"
        )

    def test_groups_merge_by_active_serving_status(self) -> None:
        """
        If two state provide sentence groups have sentences that
        have an active SERVING status at the same time,
        then they are in the same inferred group.
        """
        group_a = NormalizedStateSentenceGroup(
            state_code=self.STATE_CODE.value,
            external_id=self.GROUP_A_EXTERNAL_ID,
            sentence_group_id=hash(self.GROUP_A_EXTERNAL_ID),
            sentence_inferred_group_id=None,
        )
        group_b = NormalizedStateSentenceGroup(
            state_code=self.STATE_CODE.value,
            external_id=self.GROUP_B_EXTERNAL_ID,
            sentence_group_id=hash(self.GROUP_A_EXTERNAL_ID),
            sentence_inferred_group_id=None,
        )
        # Sentences 1 and 2 are in group A, sentence 3 is in group B
        # However, 3 begins serving before sentence 1 is terminated
        sentence_1 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_1_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_1_EXTERNAL_ID),
            sentence_group_external_id=group_a.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=2,
                ),
            ],
        )
        sentence_2 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_2_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_2_EXTERNAL_ID),
            sentence_group_external_id=group_a.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=11,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=22,
                ),
            ],
        )
        sentence_3 = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_3_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_3_EXTERNAL_ID),
            sentence_group_external_id=group_b.external_id,
            sentence_inferred_group_id=None,
            imposed_date=self.FEB_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PROBATION,
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.FEB_01),
                    status_end_datetime=None,
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=111,
                ),
            ],
        )
        actual_inferred_groups = get_normalized_inferred_sentence_groups(
            self.STATE_CODE,
            self.DELEGATE,
            normalized_sentences=[sentence_1, sentence_2, sentence_3],
        )
        inferred_group = (
            NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
                self.STATE_CODE,
                [
                    sentence_1.external_id,
                    sentence_2.external_id,
                    sentence_3.external_id,
                ],
            )
        )
        assert actual_inferred_groups == [inferred_group]
        assert (
            inferred_group.external_id == "sentence-001@#@sentence-002@#@sentence-003"
        )

    def test_groups_merge_by_charge(self) -> None:
        """
        If two sentences share a common charge, they are in the same inferred group.

        A practical example of this being useful arises when a state DOC has disparate
        systems for incarceration and supervision aspects of sentences that are imposed
        together.
        """
        common_charge = NormalizedStateChargeV2(
            state_code=self.STATE_CODE.value,
            external_id="TEST-CHARGE",
            charge_v2_id=hash("TEST-CHARGE"),
            status=StateChargeV2Status.CONVICTED,
        )
        incarceration_sentence = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_1_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_1_EXTERNAL_ID),
            sentence_group_external_id=None,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            charges=[common_charge],
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=2,
                ),
            ],
        )
        parole_sentence = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_2_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_2_EXTERNAL_ID),
            sentence_group_external_id=None,
            sentence_inferred_group_id=None,
            # This could happen in state provided data if they treat
            # parole as a separate sentence, where "imposition" is actually
            # when parole starts.
            imposed_date=self.MAR_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PAROLE,
            charges=[common_charge],
            sentence_status_snapshots=[],
        )
        actual_inferred_groups = get_normalized_inferred_sentence_groups(
            self.STATE_CODE,
            self.DELEGATE,
            normalized_sentences=[incarceration_sentence, parole_sentence],
        )
        inferred_group = (
            NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
                self.STATE_CODE,
                [incarceration_sentence.external_id, parole_sentence.external_id],
            )
        )
        assert actual_inferred_groups == [inferred_group]
        assert inferred_group.external_id == "sentence-001@#@sentence-002"

    def test_groups_merge_by_offense_date(self) -> None:
        """
        If two sentences have charges with shared offense_dates, they are in the same inferred group.

        A practical example of this being useful arises when a state DOC has disparate
        systems for incarceration and supervision aspects of sentences that are imposed
        together, with distinct identifiers used in each system.
        """
        charge_with_common_date_1 = NormalizedStateChargeV2(
            state_code=self.STATE_CODE.value,
            external_id="TEST-CHARGE-1",
            charge_v2_id=hash("TEST-CHARGE-1"),
            offense_date=datetime.date(2024, 1, 1),
            status=StateChargeV2Status.CONVICTED,
        )
        charge_with_common_date_2 = NormalizedStateChargeV2(
            state_code=self.STATE_CODE.value,
            external_id="TEST-CHARGE-2",
            charge_v2_id=hash("TEST-CHARGE-2"),
            offense_date=datetime.date(2024, 1, 1),
            status=StateChargeV2Status.CONVICTED,
        )
        incarceration_sentence = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_1_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_1_EXTERNAL_ID),
            sentence_group_external_id=None,
            sentence_inferred_group_id=None,
            imposed_date=self.JAN_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.STATE_PRISON,
            charges=[charge_with_common_date_1],
            sentence_status_snapshots=[
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.JAN_01),
                    status_end_datetime=as_datetime(self.MAR_01),
                    sequence_num=1,
                    status=StateSentenceStatus.SERVING,
                    sentence_status_snapshot_id=1,
                ),
                NormalizedStateSentenceStatusSnapshot(
                    state_code=self.STATE_CODE.value,
                    status_update_datetime=as_datetime(self.MAR_01),
                    status_end_datetime=None,
                    sequence_num=2,
                    status=StateSentenceStatus.COMPLETED,
                    sentence_status_snapshot_id=2,
                ),
            ],
        )
        parole_sentence = NormalizedStateSentence(
            state_code=self.STATE_CODE.value,
            external_id=self.SENTENCE_2_EXTERNAL_ID,
            sentence_id=hash(self.SENTENCE_2_EXTERNAL_ID),
            sentence_group_external_id=None,
            sentence_inferred_group_id=None,
            # This could happen in state provided data if they treat
            # parole as a separate sentence, where "imposition" is actually
            # when parole starts.
            imposed_date=self.MAR_01,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_type=StateSentenceType.PAROLE,
            charges=[charge_with_common_date_2],
            sentence_status_snapshots=[],
        )
        actual_inferred_groups = get_normalized_inferred_sentence_groups(
            self.STATE_CODE,
            self.DELEGATE,
            normalized_sentences=[incarceration_sentence, parole_sentence],
        )
        inferred_group = (
            NormalizedStateSentenceInferredGroup.from_sentence_external_ids(
                self.STATE_CODE,
                [incarceration_sentence.external_id, parole_sentence.external_id],
            )
        )
        assert actual_inferred_groups == [inferred_group]
        assert inferred_group.external_id == "sentence-001@#@sentence-002"
