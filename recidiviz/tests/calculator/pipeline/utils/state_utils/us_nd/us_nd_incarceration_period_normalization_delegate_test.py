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
"""Tests the US_ND-specific UsNdIncarcerationNormalizationManager."""
import datetime
import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_normalization_delegate import (
    PAROLE_REVOCATION_NORMALIZED_PREFIX,
    UsNdIncarcerationNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)

_STATE_CODE = "US_ND"


class TestNormalizedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the US_ND-specific aspects of the
    normalized_incarceration_periods_and_additional_attributes function on the
    UsNdIncarcerationNormalizationManager."""

    @staticmethod
    def _normalized_incarceration_periods_for_calculations(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        earliest_death_date: Optional[date] = None,
    ) -> List[StateIncarcerationPeriod]:
        """Helper function for testing the normalization of US_ND IPs."""
        # IP pre-processing for US_ND does not rely on violation responses
        violation_responses: Optional[
            List[NormalizedStateSupervisionViolationResponse]
        ] = []

        sp_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsNdIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=sp_index,
            normalized_violation_responses=violation_responses,
            incarceration_sentences=None,
            field_index=CoreEntityFieldIndex(),
            earliest_death_date=earliest_death_date,
        )

        (
            ips,
            _,
        ) = (
            ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
        )

        return ips

    def test_multiple_temporary_and_valid(
        self,
    ) -> None:
        state_code = _STATE_CODE
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="111",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            state_code=state_code,
            admission_date=date(2018, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="ADMN",
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            facility="CJ",
            release_date=date(2018, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id="222",
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
            state_code=state_code,
            admission_date=date(2018, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.EXTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_reason_raw_text="ADMN",
            facility="NTAD",
            release_date=date(2018, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="333",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            state_code=state_code,
            admission_date=date(2019, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        updated_temp_1 = attr.evolve(
            temporary_custody_1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        updated_temp_2 = attr.evolve(
            temporary_custody_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        incarceration_periods = [
            temporary_custody_1,
            temporary_custody_2,
            valid_incarceration_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [updated_temp_1, updated_temp_2, valid_incarceration_period],
                validated_incarceration_periods,
            )

    def test_multiple_temporary_and_transfer(
        self,
    ) -> None:
        state_code = _STATE_CODE
        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="1",
            state_code=state_code,
            facility="NTAD",
            custodial_authority=StateCustodialAuthority.EXTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
            admission_date=date(2018, 11, 20),
            admission_reason_raw_text="NTAD",
            release_date=date(2018, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id="2",
            state_code=state_code,
            facility="CJ",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            custodial_authority=StateCustodialAuthority.COUNTY,
            admission_date=date(2018, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="ADMN",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            release_date=date(2018, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
        )

        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="3",
            state_code=state_code,
            admission_date=date(2020, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2020, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="4",
            state_code=state_code,
            admission_date=date(2020, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2020, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="5",
            state_code=state_code,
            admission_date=date(2020, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2020, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [
            temporary_custody_1,
            temporary_custody_2,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
        ]

        updated_periods = [
            attr.evolve(
                temporary_custody_1,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            ),
            attr.evolve(
                temporary_custody_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            ),
            attr.evolve(valid_incarceration_period_1),
            attr.evolve(valid_incarceration_period_2),
            attr.evolve(valid_incarceration_period_3),
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                updated_periods,
                validated_incarceration_periods,
            )

    def test_handle_temporary_custody(
        self,
    ) -> None:
        state_code = _STATE_CODE

        county_jail_pre_2017 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id="2",
            state_code=state_code,
            facility="CJ",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="ADMN",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2008, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
        )

        temporary_custody_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="1",
            state_code=state_code,
            facility="NTAD",
            custodial_authority=StateCustodialAuthority.EXTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
            admission_date=date(2018, 11, 20),
            admission_reason_raw_text="NTAD",
            release_date=date(2018, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
        )

        temporary_custody_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=112,
            external_id="2",
            state_code=state_code,
            facility="CJ",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            custodial_authority=StateCustodialAuthority.COUNTY,
            admission_date=date(2018, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="ADMN",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            release_date=date(2018, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPAR",
        )

        incarceration_periods = [
            county_jail_pre_2017,
            temporary_custody_1,
            temporary_custody_2,
        ]

        updated_temp_1 = attr.evolve(
            temporary_custody_1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        updated_temp_2 = attr.evolve(
            temporary_custody_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            validated_incarceration_periods = (
                self._normalized_incarceration_periods_for_calculations(
                    incarceration_periods=ips_for_test
                )
            )

            self.assertEqual(
                [
                    county_jail_pre_2017,
                    updated_temp_1,
                    updated_temp_2,
                ],
                validated_incarceration_periods,
            )

    def test_invalid_board_hold(
        self,
    ) -> None:
        state_code = _STATE_CODE
        invalid_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [invalid_board_hold]

        with self.assertRaises(ValueError):
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )

    def test_NewAdmissionAfterProbationRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period directly, then the admission_reason on the period is
        updated to be REVOCATION."""
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_NewAdmissionAfterParoleRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PAROLE+REVOCATION
        supervision period directly, then the admission_reason on the period is
        updated to be REVOCATION."""
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_NewAdmissionAfterIrrelevantSupervisionType(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a
        INVESTIGATION+NOT REVOCATION supervision period directly, then the
        incarceration period is not updated."""
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_NewAdmissionNotAfterRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+NOT REVOCATION
        supervision period directly, then the incarceration period is not updated."""
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(2019, 3, 5),
            termination_date=date(2019, 6, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_NewAdmissionNotDirectlyAfterProbationRevocation(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a INVESTIGATION+REVOCATION
        supervision period in between them, then the incarceration period is not
        updated."""
        earlier_probation_supervision_period = (
            NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                sequence_num=0,
                external_id="sp1",
                state_code=_STATE_CODE,
                start_date=date(2019, 3, 5),
                termination_date=date(2019, 5, 4),
                termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            )
        )

        later_parole_supervision_period = (
            NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                sequence_num=1,
                external_id="sp1",
                state_code=_STATE_CODE,
                start_date=date(2019, 3, 5),
                termination_date=date(2019, 6, 9),
                termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2019, 6, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[incarceration_period],
                supervision_periods=[
                    earlier_probation_supervision_period,
                    later_parole_supervision_period,
                ],
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_NewAdmissionAfterRevocationAndIntermediateAdmissionToStatePrison(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a separate incarceration
        admission in the interim, then the subsequent incarceration period is not
        updated."""
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(1996, 3, 5),
            termination_date=date(2000, 1, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        initial_commitment_incarceration_period = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                external_id="ip2",
                state_code=_STATE_CODE,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                admission_date=date(2000, 2, 9),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2000, 12, 24),
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            )
        )

        subsequent_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2001, 10, 14),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_initial_period = attr.evolve(
            initial_commitment_incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        subsequent_period_updated = attr.evolve(
            subsequent_incarceration_period,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
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

    def test_NewAdmissionAfterRevocationAndIntermediateAdmissionToCountyJail(
        self,
    ) -> None:
        """Tests that when a NEW_ADMISSION incarceration follows a PROBATION+REVOCATION
        supervision period, but not directly, as there is a separate incarceration
        admission in the interim, but that admission is to a county jail (i.e. is a
        temporary hold while revocation proceedings take place), then the
        admission_reason on the period is updated to be REVOCATION."""
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=_STATE_CODE,
            start_date=date(1996, 3, 5),
            termination_date=date(2018, 1, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        initial_commitment_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            facility="CJ",
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_date=date(2018, 1, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2018, 4, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        subsequent_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2018, 4, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        updated_period = attr.evolve(
            subsequent_incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[
                    initial_commitment_incarceration_period,
                    subsequent_incarceration_period,
                ],
                supervision_periods=[supervision_period],
            )
        )

        self.assertEqual(
            [initial_commitment_incarceration_period, updated_period],
            validated_incarceration_periods,
        )

    def test_TakeNewAdmissionFromTemporaryCustody(self) -> None:
        # Too long of a time gap between date_1 and date_2 to be
        # considered consecutive
        date_1 = date(2020, 1, 1)
        date_2 = date_1 + datetime.timedelta(days=3)
        date_3 = date_2 + datetime.timedelta(days=1)

        ip = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="1",
            state_code=_STATE_CODE,
            admission_date=date_1,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            facility="CJ",
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="PV",
            release_date=date_1,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2,
            external_id="2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            facility="CJ",
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_date=date_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date_2,
            admission_reason_raw_text="ADMN",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        ip_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3,
            external_id="3",
            state_code=_STATE_CODE,
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_date=date_3,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INT",
            release_date=date_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        expected_ip = attr.evolve(
            ip,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )
        expected_ip_2 = attr.evolve(
            ip_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )
        expected_ip_3 = attr.evolve(
            ip_3,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[ip, ip_2, ip_3]
            )
        )

        self.assertEqual(
            [expected_ip, expected_ip_2, expected_ip_3],
            validated_incarceration_periods,
        )

    def test_TakeRevocationFromTemporaryCustody(self) -> None:
        # All periods are consecutive
        date_1 = date(2020, 1, 1)
        date_2 = date_1 + datetime.timedelta(days=1)
        date_3 = date_2 + datetime.timedelta(days=1)

        ip = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="1",
            state_code=_STATE_CODE,
            admission_date=date_1,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            facility="CJ",
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="PV",
            release_date=date_1,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2,
            external_id="2",
            state_code=_STATE_CODE,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            facility="CJ",
            custodial_authority=StateCustodialAuthority.COUNTY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_date=date_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date_2,
            admission_reason_raw_text="ADMN",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        ip_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3,
            external_id="3",
            state_code=_STATE_CODE,
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_date=date_3,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INT",
            release_date=date_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        expected_ip = attr.evolve(
            ip,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        expected_ip_2 = attr.evolve(
            ip_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        assert ip_3.admission_reason_raw_text is not None
        expected_ip_3 = attr.evolve(
            ip_3,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text=(
                PAROLE_REVOCATION_NORMALIZED_PREFIX
                + "-"
                + ip_3.admission_reason_raw_text
            ),
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[ip, ip_2, ip_3]
            )
        )

        self.assertEqual(
            [expected_ip, expected_ip_2, expected_ip_3],
            validated_incarceration_periods,
        )

    def test_doNotTakeNullAdmissionFromTemporaryCustody(self) -> None:
        # Periods are consecutive
        date_1 = date(2020, 1, 1)
        date_2 = date_1 + datetime.timedelta(days=1)

        ip = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="1",
            state_code=_STATE_CODE,
            admission_date=date_1,
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
            facility="NTAD",
            custodial_authority=StateCustodialAuthority.EXTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_reason=None,
            # This raw text value is in the enum ignores
            admission_reason_raw_text="NTAD",
            release_date=date_1,
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3,
            external_id="3",
            state_code=_STATE_CODE,
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_date=date_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INT",
            release_date=date_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        expected_ip = attr.evolve(
            ip,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )
        # We won't take the admission reason from the first adjacent temp custody
        # period if there's no admission reason on it
        expected_ip_2 = attr.evolve(
            ip_2,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[ip, ip_2]
            )
        )

        self.assertEqual(
            [expected_ip, expected_ip_2],
            validated_incarceration_periods,
        )

    def test_HandleErroneouslySetTemporaryCustody(self) -> None:
        # Periods are consecutive
        date_1 = date(2020, 1, 1)
        date_2 = date_1 + datetime.timedelta(days=1)

        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            external_id="1",
            state_code=_STATE_CODE,
            facility="NDSP",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_date=date_1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INT",
            release_date=date_1,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        # This period isn't actually a temporary custody period since it follows
        # directly after a state prisonn stay.
        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3,
            external_id="3",
            state_code=_STATE_CODE,
            admission_date=date_2,
            incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
            facility="NTAD",
            custodial_authority=StateCustodialAuthority.EXTERNAL_UNKNOWN,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            admission_reason=None,
            # This raw text value is in the enum ignores
            admission_reason_raw_text="NTAD",
            release_date=date_2,
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        expected_ip_1 = attr.evolve(
            ip_1,
        )

        expected_ip_2 = attr.evolve(
            ip_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[ip_1, ip_2]
            )
        )

        self.assertEqual(
            [expected_ip_1, expected_ip_2],
            validated_incarceration_periods,
        )
