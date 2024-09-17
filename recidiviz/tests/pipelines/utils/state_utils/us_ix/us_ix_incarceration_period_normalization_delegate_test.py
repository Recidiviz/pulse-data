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
"""Tests the US_IX-specific UsIxIncarcerationNormalizationManager."""
import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr
import mock

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_incarceration_period_normalization_delegate import (
    UsIxIncarcerationNormalizationDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)


class TestNormalizedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_IX-specific aspects of the
    normalized_incarceration_periods_and_additional_attributes function on the
    UsIxIncarcerationNormalizationManager."""

    def setUp(self) -> None:
        self.delegate = UsIxIncarcerationNormalizationDelegate()
        self.person_id = 9000000000000000123

        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils.generate_primary_key"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 9000000000012312345

    def tearDown(self) -> None:
        self.unique_id_patcher.stop()

    @staticmethod
    def _normalized_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        """Helper function for testing the normalization of US_IX IPs."""
        # IP pre-processing for US_IX does not rely on violation responses
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsIxIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=sp_index,
            normalized_violation_responses=violation_responses,
            person_id=123,
            earliest_death_date=earliest_death_date,
        )

        (
            ips,
            _,
        ) = (
            ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
        )

        return ips

    def test_normalized_incarceration_periods_different_pfi_do_not_collapse(
        self,
    ) -> None:
        """Tests the pre-processing function does not collapse two adjacent TRANSFER
        edges in US_IX when they have different specialized_purpose_for_incarceration
        values.
        """
        state_code = "US_IX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            state_code=state_code,
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            second_incarceration_period,
        ]

        updated_periods = [
            attr.evolve(
                initial_incarceration_period,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                second_incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_normalized_incarceration_periods_same_pfi_transfer(
        self,
    ) -> None:
        """Tests the pre-processing function doesn't apply STATUS_CHANGE edges to
        TRANSFERS in US_IX when they have the same specialized_purpose_for_incarceration
        values.
        """
        state_code = "US_IX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            state_code=state_code,
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            initial_incarceration_period,
            second_incarceration_period,
        ]

        updated_periods = [
            attr.evolve(initial_incarceration_period),
            attr.evolve(second_incarceration_period),
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(validated_incarceration_periods, updated_periods)

    def test_normalized_incarceration_periods_commitment_with_general_purpose(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_normalized_incarceration_periods_commitment_with_treatment(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period]
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_normalized_incarceration_periods_commitment_with_treatment_transfer(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_IX",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        general_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_periods = [
            attr.evolve(
                treatment_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                general_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[general_period, treatment_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_normalized_incarceration_periods_admission_from_investigation(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_IX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2018, 5, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_periods = [
            attr.evolve(
                incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            ),
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_normalized_incarceration_periods_purpose_for_incarceration_change_us_ix(
        self,
    ) -> None:
        """Tests that with state code US_IX, treatment in prison periods that are followed by a transfer to general
        result in the correctly updated STATUS_CHANGE reasons."""

        treatment_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_IX",
            external_id="1",
            facility="PRISON3",
            admission_date=date(2009, 11, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2009, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        general_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_IX",
            external_id="2",
            facility="PRISON 10",
            admission_date=date(2009, 12, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        updated_periods = [
            attr.evolve(
                treatment_incarceration_period,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                general_incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[
                    treatment_incarceration_period,
                    general_incarceration_period,
                ],
                supervision_periods=[],
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_us_ix_normalize_period_if_commitment_from_supervision_probation_revocation(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        expected_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_revocation_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([expected_period], validated_incarceration_periods)

    def test_us_ix_normalize_period_if_commitment_from_supervision_treatment(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        expected_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_revocation_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([expected_period], validated_incarceration_periods)

    def test_us_ix_sanction_admission_shock_incarceration(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        shock_incarceration_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        with self.assertRaises(ValueError):
            # We don't expect to see SHOCK_INCARCERATION sanction admissions in US_IX
            _ = self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[shock_incarceration_admission],
                supervision_periods=[supervision_period],
            )

    def test_us_ix_normalize_period_if_commitment_from_supervision_parole_board_revocation(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_board_hold_period = attr.evolve(
            board_hold_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        updated_revocation_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[
                    board_hold_period,
                    incarceration_revocation_period,
                ],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual(
            [updated_board_hold_period, updated_revocation_period],
            validated_incarceration_periods,
        )

    def test_us_ix_normalize_period_if_commitment_from_supervision_parole_board_to_treatment_revocation(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        updated_board_hold_period = attr.evolve(
            board_hold_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        updated_sanction_period = attr.evolve(
            incarceration_revocation_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[
                    board_hold_period,
                    incarceration_revocation_period,
                ],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual(
            [updated_board_hold_period, updated_sanction_period],
            validated_incarceration_periods,
        )

    def test_us_ix_normalize_period_if_commitment_from_supervision_treatment_transfer_not_revocation(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        transfer_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2018, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_treatment_period = attr.evolve(
            treatment_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
        )

        updated_transfer_period = attr.evolve(
            transfer_incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[treatment_period, transfer_incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual(
            [updated_treatment_period, updated_transfer_period],
            validated_incarceration_periods,
        )

    def test_us_ix_normalize_period_if_commitment_from_supervision_no_revocation(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(incarceration_period)

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period]
            )
        )

        self.assertEqual([period_copy], validated_incarceration_periods)

    def test_us_ix_normalize_period_if_commitment_from_supervision_transfer_admission(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(
            incarceration_period,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period]
            )
        )

        self.assertEqual([period_copy], validated_incarceration_periods)

    def test_us_ix_normalize_period_if_commitment_from_supervision_investigation_not_revocation(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            supervision_period_id=111,
            external_id="sp1",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        expected_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([expected_period], validated_incarceration_periods)

    # Test adding IP for an open IN_CUSTODY supervision period when a person has no incarceration periods
    def test_inferred_incarceration_periods_from_open_IC_supervision_period_with_no_ips(
        self,
    ) -> None:
        self.maxDiff = None
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_IX",
            start_date=date(2017, 3, 5),
            termination_date=None,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=9000000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 3, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=None,
            release_reason=None,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period]
            ),
        )

        expected_periods = [new_period]
        self.assertEqual(expected_periods, additional_ip)

    # Test adding IP for a closed IN_CUSTODY supervision period when a person has no incarceration periods
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_with_no_ips(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_IX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 4, 20),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        new_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=9000000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 3, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 4, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period]
            ),
        )

        expected_periods = [new_period]
        self.assertEqual(expected_periods, additional_ip)

    # Test adding IP for a closed IN_CUSTODY supervision period when a person has an incarceration period admission date that starts on the same day as the SP termination date
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_that_abutts_IP_start(
        self,
    ) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_IX",
            start_date=date(2017, 4, 20),
            termination_date=date(2017, 4, 30),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip1",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        new_inferred_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=9000000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 4, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 4, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period_1]
            ),
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_inferred_period,
        ]
        self.assertEqual(expected_periods, additional_ip)

    # Test not adding IP for a closed IN_CUSTODY supervision period when a person has an incarceration period admission date that starts after SP termination date
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_that_terminates_before_IP_start(
        self,
    ) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_IX",
            start_date=date(2017, 4, 20),
            termination_date=date(2017, 4, 25),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip1",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        new_inferred_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=9000000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 4, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 4, 25),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period_1]
            ),
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_inferred_period,
        ]
        self.assertEqual(expected_periods, additional_ip)

    # Test adding IP for a closed IN_CUSTODY supervision period when a person has an incarceration period admission date that is before SP termination date but within 3 months of start
    def test_inferred_incarceration_periods_from_closed_IC_supervision_period_that_overlaps_IP_start(
        self,
    ) -> None:
        supervision_period_1 = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_IX",
            start_date=date(2017, 4, 20),
            termination_date=date(2017, 5, 7),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            sequence_num=0,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip1",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 4, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2018, 9, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=333,
            external_id="ip2",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 9, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        new_inferred_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=9000000000012312345,
            external_id="sp1-0-IN-CUSTODY",
            state_code="US_IX",
            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            admission_date=date(2017, 4, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            # Inferred period ends when the first incarceration period starts
            release_date=date(2017, 4, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        additional_ip = self.delegate.infer_additional_periods(
            person_id=self.person_id,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=[supervision_period_1]
            ),
        )

        expected_periods = [
            incarceration_period_1,
            incarceration_period_2,
            new_inferred_period,
        ]
        self.assertEqual(expected_periods, additional_ip)
