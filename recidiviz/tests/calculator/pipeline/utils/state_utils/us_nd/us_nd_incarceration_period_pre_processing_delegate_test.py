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
"""Tests the US_ND-specific UsNdIncarcerationPreProcessingManager."""
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
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_pre_processing_delegate import (
    UsNdIncarcerationPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
)


class TestPreProcessedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_ND-specific aspects of the
    pre_processed_incarceration_periods_for_calculations function on the
    UsNdIncarcerationPreProcessingManager."""

    @staticmethod
    def _pre_processed_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        collapse_transfers: bool = True,
        overwrite_facility_information_in_transfers: bool = True,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        # IP pre-processing for US_ND does not rely on violation responses
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = []

        sp_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods or [],
        )

        ip_pre_processing_manager = IncarcerationPreProcessingManager(
            incarceration_periods=incarceration_periods,
            delegate=UsNdIncarcerationPreProcessingDelegate(),
            pre_processed_supervision_period_index=sp_index,
            violation_responses=violation_responses,
            earliest_death_date=earliest_death_date,
        )

        return ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        ).incarceration_periods

    def test_pre_processed_incarceration_periods_for_calculations_multiple_temporary_and_valid(
        self,
    ) -> None:
        state_code = "US_ND"
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id="222",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="333",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            admission_date=date(2016, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2017, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            temporary_custody_1,
            temporary_custody_2,
            valid_incarceration_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                )
            )

            self.assertEqual(
                [valid_incarceration_period], validated_incarceration_periods
            )

    def test_pre_processed_incarceration_periods_for_calculations_multiple_temporary_and_transfer(
        self,
    ) -> None:
        state_code = "US_ND"
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="2",
            state_code=state_code,
            admission_date=date(2008, 12, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 12, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="3",
            state_code=state_code,
            admission_date=date(2011, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=ReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="4",
            state_code=state_code,
            admission_date=date(2012, 12, 4),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=ReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="5",
            state_code=state_code,
            admission_date=date(2012, 12, 24),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            temporary_custody_1,
            temporary_custody_2,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
        ]

        collapsed_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=valid_incarceration_period_1.incarceration_period_id,
            external_id=valid_incarceration_period_1.external_id,
            status=valid_incarceration_period_3.status,
            state_code=valid_incarceration_period_1.state_code,
            admission_date=valid_incarceration_period_1.admission_date,
            admission_reason=valid_incarceration_period_1.admission_reason,
            release_date=valid_incarceration_period_3.release_date,
            release_reason=valid_incarceration_period_3.release_reason,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                )
            )

            self.assertEqual(
                [collapsed_incarceration_period], validated_incarceration_periods
            )

    def test_pre_processed_incarceration_periods_for_calculations_valid_then_temporary(
        self,
    ) -> None:
        state_code = "US_ND"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        temporary_custody = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="2",
            state_code=state_code,
            admission_date=date(2010, 1, 20),
            admission_reason=AdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 1, 24),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        updated_valid_period = attr.evolve(
            valid_incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [valid_incarceration_period, temporary_custody]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._pre_processed_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test,
                )
            )

            self.assertEqual([updated_valid_period], validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_invalid_board_hold(
        self,
    ) -> None:
        state_code = "US_ND"
        invalid_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=AdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2009, 12, 4),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [invalid_board_hold]

        with self.assertRaises(ValueError):
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
            )

    def test_pre_processed_incarceration_periods_for_calculations_NewAdmissionAfterProbationRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period directly, then the admission_reason on the period is
        updated to be PROBATION_REVOCATION."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_NewAdmissionNotAfterProbation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PAROLE+NOT REVOCATION
        supervision period directly, then the incarceration period is not updated."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_NewAdmissionNotAfterRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+NOT REVOCATION
        supervision period directly, then the incarceration period is not updated."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_NewAdmissionNotDirectlyAfterProbationRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a PAROLE+REVOCATION supervision
        period in between them, then the incarceration period is not updated."""
        earlier_probation_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 5, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        later_parole_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[
                    earlier_probation_supervision_period,
                    later_parole_supervision_period,
                ],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_pre_processed_incarceration_periods_for_calculations_NewAdmissionAfterRevocationAndIntermediateAdmissionToStatePrison(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a separate incarceration
        admission in the interim, then the subsequent incarceration period is not
        updated."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(1996, 3, 5),
            termination_date=date(2000, 1, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        initial_commitment_incarceration_period = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                external_id="ip2",
                state_code="US_ND",
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=date(2000, 2, 9),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2000, 12, 24),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            )
        )

        subsequent_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2001, 10, 14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_initial_period = attr.evolve(
            initial_commitment_incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        subsequent_period_updated = attr.evolve(
            subsequent_incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[
                    initial_commitment_incarceration_period,
                    subsequent_incarceration_period,
                ],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual(
            [updated_initial_period, subsequent_period_updated],
            validated_incarceration_periods,
        )

    def test_pre_processed_incarceration_periods_for_calculations_NewAdmissionAfterRevocationAndIntermediateAdmissionToCountyJail(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a separate incarceration
        admission in the interim, but that admission is to a county jail (i.e. is a
        temporary hold while revocation proceedings take place, then the
        admission_reason on the period is updated to be PROBATION_REVOCATION."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(1996, 3, 5),
            termination_date=date(2000, 1, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        initial_commitment_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2000, 1, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2000, 4, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        subsequent_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=date(2000, 4, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            subsequent_incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._pre_processed_incarceration_periods_for_calculations(
                incarceration_periods=[
                    initial_commitment_incarceration_period,
                    subsequent_incarceration_period,
                ],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)
