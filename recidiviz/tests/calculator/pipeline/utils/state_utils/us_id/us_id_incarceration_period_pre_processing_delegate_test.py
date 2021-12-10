# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests the US_ID-specific UsIdIncarcerationPreProcessingManager."""
import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    IncarcerationPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_delegate import (
    UsIdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_period_pre_processing_delegate import (
    UsIdIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
)


class TestPreProcessedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_ID-specific aspects of the
    pre_processed_incarceration_periods_for_calculations function on the
    UsIdIncarcerationPreProcessingManager."""

    @staticmethod
    def _pre_processed_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        collapse_transfers: bool = True,
        overwrite_facility_information_in_transfers: bool = True,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        # IP pre-processing for US_ID does not rely on violation responses
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = []

        sp_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods or [],
        )

        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            pre_processing_delegate=UsIdIncarcerationPreProcessingDelegate(),
            incarceration_delegate=UsIdIncarcerationDelegate(),
            pre_processed_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=earliest_death_date,
            field_index=CoreEntityFieldIndex(),
        )

        return ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        ).incarceration_periods

    def test_pre_processed_incarceration_periods_different_pfi_do_not_collapse(
        self,
    ) -> None:
        """Tests the pre-processing function does not collapse two adjacent TRANSFER
        edges in US_ID when they have different specialized_purpose_for_incarceration
        values.
        """
        state_code = "US_ID"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                collapse_transfers=True,
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_same_pfi_transfer(
        self,
    ) -> None:
        """Tests the pre-processing function does collapse two adjacent TRANSFER
        edges in US_ID when they have the same specialized_purpose_for_incarceration
        values.
        """
        state_code = "US_ID"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
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

        collapsed_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=initial_incarceration_period.incarceration_period_id,
            state_code=state_code,
            admission_date=initial_incarceration_period.admission_date,
            admission_reason=initial_incarceration_period.admission_reason,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=second_incarceration_period.release_date,
            release_reason=second_incarceration_period.release_reason,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                    collapse_transfers=True,
                )
            )

            self.assertEqual(validated_incarceration_periods, [collapsed_period])

    def test_pre_processed_incarceration_periods_commitment_with_general_purpose(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_commitment_with_treatment(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_commitment_with_treatment_transfer(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_ID",
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[general_period, treatment_period],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_admission_from_investigation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual(updated_periods, validated_incarceration_periods)

    def test_us_id_normalize_period_if_commitment_from_supervision_probation_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_revocation_period],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual([expected_period], validated_incarceration_periods)

    def test_us_id_normalize_period_if_commitment_from_supervision_treatment(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_revocation_period],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual([expected_period], validated_incarceration_periods)

    def test_us_id_sanction_admission_shock_incarceration(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        shock_incarceration_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        with self.assertRaises(ValueError):
            # We don't expect to see SHOCK_INCARCERATION sanction admissions in US_ID
            _ = self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[shock_incarceration_admission],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )

    def test_us_id_normalize_period_if_commitment_from_supervision_parole_board_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[
                    board_hold_period,
                    incarceration_revocation_period,
                ],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual(
            [updated_board_hold_period, updated_revocation_period],
            validated_incarceration_periods,
        )

    def test_us_id_normalize_period_if_commitment_from_supervision_parole_board_to_treatment_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[
                    board_hold_period,
                    incarceration_revocation_period,
                ],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual(
            [updated_board_hold_period, updated_sanction_period],
            validated_incarceration_periods,
        )

    def test_us_id_normalize_period_if_commitment_from_supervision_treatment_transfer_not_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[treatment_period, transfer_incarceration_period],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual(
            [updated_treatment_period, updated_transfer_period],
            validated_incarceration_periods,
        )

    def test_us_id_normalize_period_if_commitment_from_supervision_no_revocation(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 5, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        period_copy = attr.evolve(incarceration_period)

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual([period_copy], validated_incarceration_periods)

    def test_us_id_normalize_period_if_commitment_from_supervision_transfer_admission(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual([period_copy], validated_incarceration_periods)

    def test_us_id_normalize_period_if_commitment_from_supervision_investigation_not_revocation(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
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
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
                collapse_transfers=True,
            )
        )

        self.assertEqual([expected_period], validated_incarceration_periods)
