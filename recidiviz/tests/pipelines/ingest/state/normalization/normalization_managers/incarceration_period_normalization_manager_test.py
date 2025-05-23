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
# pylint: disable=protected-access
"""Tests for incarceration_period_normalization_manager.py."""

import unittest
from datetime import date
from itertools import permutations
from typing import List, Optional

import attr
from freezegun import freeze_time

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    ATTRIBUTES_TRIGGERING_STATUS_CHANGE,
    IncarcerationPeriodNormalizationManager,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_incarceration_period_normalization_delegate import (
    UsXxIncarcerationNormalizationDelegate,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)


class TestNormalizedIncarcerationPeriodsForCalculations(unittest.TestCase):
    """Tests the normalized_incarceration_periods_and_additional_attributes function."""

    def _normalized_incarceration_periods_for_calculations(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        earliest_death_date: Optional[date] = None,
        normalization_delegate_override: Optional[
            StateSpecificIncarcerationNormalizationDelegate
        ] = None,
    ) -> List[StateIncarcerationPeriod]:
        """Normalizes incarceration periods for calculations"""
        # None of the state-agnostic tests rely on supervision periods
        sp_index = default_normalized_sp_index_for_tests([])
        # None of the state-agnostic tests rely on violation responses
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=(
                normalization_delegate_override
                or UsXxIncarcerationNormalizationDelegate()
            ),
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

    def test_normalized_incarceration_periods_for_calculations(self) -> None:
        state_code = "US_XX"
        initial_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        first_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2011, 3, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        second_reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2012, 2, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2016, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            initial_incarceration_period,
            first_reincarceration_period,
            second_reincarceration_period,
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(validated_incarceration_periods, incarceration_periods)

    def test_normalized_incarceration_periods_for_calculations_standardize_parole_board_hold(
        self,
    ) -> None:
        state_code = "US_XX"
        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        reincarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2015, 2, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2016, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            board_hold,
            revocation_period,
            reincarceration_period,
        ]

        updated_board_hold = attr.evolve(
            board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(
            [updated_board_hold, revocation_period, reincarceration_period],
            validated_incarceration_periods,
        )

    def test_normalized_incarceration_periods_for_calculations_standardize_parole_board_hold_no_release(
        self,
    ) -> None:
        state_code = "US_XX"
        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        incarceration_periods = [
            board_hold,
        ]

        updated_board_hold = attr.evolve(
            board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(
            [updated_board_hold],
            validated_incarceration_periods,
        )

    def test_normalized_incarceration_periods_for_calculations_board_hold_other_temp_custody(
        self,
    ) -> None:
        state_code = "US_XX"

        temporary_custody_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        temporary_custody_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            release_date=date(2008, 11, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [
            board_hold,
            temporary_custody_period_1,
            temporary_custody_period_2,
        ]

        updated_temporary_custody_1 = attr.evolve(
            temporary_custody_period_1,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        updated_temporary_custody_2 = attr.evolve(
            temporary_custody_period_2,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        updated_board_hold = attr.evolve(
            board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(
            [
                updated_temporary_custody_1,
                updated_temporary_custody_2,
                updated_board_hold,
            ],
            validated_incarceration_periods,
        )

    def test_normalized_incarceration_periods_for_calculations_not_board_hold_other_temp_custody(
        self,
    ) -> None:
        state_code = "US_XX"

        not_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        incarceration_periods = [not_board_hold]

        updated_period = attr.evolve(
            not_board_hold,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(
            [updated_period],
            validated_incarceration_periods,
        )

    def test_parole_board_hold_edges_stay_as_transfers(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(incarceration_period_1),
            attr.evolve(incarceration_period_2),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_parole_board_hold_edges_updated_as_transfers(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_parole_board_hold_should_not_be_updated_as_transfers(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2019, 12, 28),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
            ),
            attr.evolve(
                incarceration_period_2,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_update_edges_to_transfers(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_no_update_for_edges_to_transfers_when_gap_in_between(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2020, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=None,
            release_date=date(2020, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
            attr.evolve(
                incarceration_period_2,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_normalized_incarceration_periods_for_calculations_drop_invalid_zero_day(
        self,
    ) -> None:
        state_code = "US_XX"
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        invalid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="3",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 12, 19),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            valid_incarceration_period_1,
            invalid_incarceration_period,
            valid_incarceration_period_2,
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(
            [valid_incarceration_period_1, valid_incarceration_period_2],
            validated_incarceration_periods,
        )

    def test_normalized_incarceration_periods_for_calculations_drop_periods_for_deceased(
        self,
    ) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2009, 12, 5),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2009, 12, 6),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_3,
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods,
                earliest_death_date=incarceration_period_1.release_date,
            )
        )

        self.assertEqual(
            [incarceration_period_1],
            validated_incarceration_periods,
        )

    def test_normalized_incarceration_periods_for_calculations_drop_invalid_zero_day_after_transfer(
        self,
    ) -> None:
        state_code = "US_XX"
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="2",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        invalid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="3",
            state_code=state_code,
            admission_date=date(2009, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2009, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            valid_incarceration_period_1,
            invalid_incarceration_period,
            valid_incarceration_period_2,
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        updated_periods = [
            attr.evolve(valid_incarceration_period_1),
            attr.evolve(valid_incarceration_period_2),
        ]

        self.assertEqual(
            updated_periods,
            validated_incarceration_periods,
        )

    def test_normalized_incarceration_periods_for_calculations_drop_invalid_zero_day_border(
        self,
    ) -> None:
        zero_day_start = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        valid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        zero_day_end_different_reason = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [
            zero_day_start,
            valid_period,
            zero_day_end_different_reason,
        ]

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=incarceration_periods
            )
        )

        self.assertEqual(
            [valid_period, zero_day_end_different_reason],
            validated_incarceration_periods,
        )

    def test_apply_overrides_sanction_admission(self) -> None:
        sanction_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        updated_period = attr.evolve(
            sanction_admission,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[sanction_admission]
            )
        )

        self.assertEqual([updated_period], validated_incarceration_periods)

    def test_apply_overrides_not_sanction_admission(self) -> None:
        regular_admission = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            # This person was sentenced to complete treatment in prison
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        period_copy = attr.evolve(
            regular_admission,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[regular_admission]
            )
        )

        self.assertEqual([period_copy], validated_incarceration_periods)

    @freeze_time("2000-01-01 00:00:00-05:00")
    def test_normalized_incarceration_periods_for_calculations_dates_in_future(
        self,
    ) -> None:
        incarceration_period_in_future_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(1990, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            # Erroneous release_date in the future
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_in_future_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            # Erroneous admission_date in the future, period should be dropped entirely
            admission_date=date(2010, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2010, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[
                    incarceration_period_in_future_1,
                    incarceration_period_in_future_2,
                ]
            )
        )

        expected_periods = [
            attr.evolve(
                incarceration_period_in_future_1, release_date=None, release_reason=None
            )
        ]

        self.assertEqual(expected_periods, validated_incarceration_periods)

    def test_normalized_incarceration_periods_for_calculations_transfer_status_change(
        self,
    ) -> None:
        """Tests that adjacent IPs with TRANSFER edges are updated to have STATUS_CHANGE
        release and admission reasons when the IPs have different
        specialized_purpose_for_incarceration values.
        """
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        validated_incarceration_periods = (
            self._normalized_incarceration_periods_for_calculations(
                incarceration_periods=[
                    incarceration_period_1,
                    incarceration_period_2,
                ]
            )
        )

        expected_periods = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        self.assertEqual(expected_periods, validated_incarceration_periods)

    def test_add_inferred_periods_even_if_there_are_no_original_ips(self) -> None:
        """Tests that if the delegate returns additional inferred IPs, those are
        returned, even if there are no input IPs.
        """
        inferred_ip = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        class _UsXxIncarcerationNormalizationDelegate(
            StateSpecificIncarcerationNormalizationDelegate
        ):
            def infer_additional_periods(
                self,
                person_id: int,
                incarceration_periods: List[StateIncarcerationPeriod],
                supervision_period_index: NormalizedSupervisionPeriodIndex,
            ) -> List[StateIncarcerationPeriod]:
                return [inferred_ip]

        validated_incarceration_periods = self._normalized_incarceration_periods_for_calculations(
            incarceration_periods=[],
            normalization_delegate_override=_UsXxIncarcerationNormalizationDelegate(),
        )
        self.assertEqual([inferred_ip], validated_incarceration_periods)

    def test_additional_attributes_map(self) -> None:
        incarceration_periods = [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id="1",
                state_code="US_XX",
                admission_date=date(2012, 12, 24),
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                release_date=date(2012, 12, 24),
                release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            )
        ]

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsXxIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=default_normalized_sp_index_for_tests(
                []
            ),
            normalized_violation_responses=[],
            person_id=123,
            earliest_death_date=None,
        )

        (
            _,
            attributes_map,
        ) = (
            ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
        )

        expected_attributes_map = {
            StateIncarcerationPeriod.__name__: {
                111: {
                    "sequence_num": 0,
                    "purpose_for_incarceration_subtype": None,
                    "incarceration_admission_violation_type": None,
                }
            }
        }

        self.assertEqual(expected_attributes_map, attributes_map)


class TestSortAndInferMissingDatesAndStatuses(unittest.TestCase):
    """Tests the _infer_missing_dates_and_statuses."""

    @staticmethod
    def _sort_and_infer_missing_dates_and_statuses(
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        # None of the state-agnostic tests rely on violation responses
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        ip_normalization_manager = IncarcerationPeriodNormalizationManager(
            incarceration_periods=incarceration_periods,
            normalization_delegate=UsXxIncarcerationNormalizationDelegate(),
            normalized_supervision_period_index=default_normalized_sp_index_for_tests(
                []
            ),
            normalized_violation_responses=violation_responses,
            person_id=123,
            earliest_death_date=None,
        )

        return ip_normalization_manager._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods,
        )

    def test_sort_and_infer_missing_dates_and_statuses(self) -> None:
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        # Invalid open period with same admission date as valid_incarceration_period_1
        invalid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incarceration_periods = [
            valid_incarceration_period_3,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            invalid_open_period,
        ]

        updated_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            # Release date set to the admission date of valid_incarceration_period_1
            release_date=valid_incarceration_period_1.admission_date,
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [
                    updated_open_period,
                    valid_incarceration_period_1,
                    valid_incarceration_period_2,
                    valid_incarceration_period_3,
                ],
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_all_valid(self) -> None:
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="4",
            state_code="US_XX",
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        # Valid open period
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="6",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [
            valid_open_period,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
        ]

        ordered_periods = [
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
            valid_open_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(ordered_periods, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_all_valid_shared_release(
        self,
    ) -> None:
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="4",
            state_code="US_XX",
            admission_date=date(2012, 11, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [
            valid_incarceration_period,
            nested_incarceration_period,
        ]

        ordered_periods = [valid_incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(ordered_periods, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_set_empty_reasons(self) -> None:
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=None,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="4",
            state_code="US_XX",
            admission_date=date(2012, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        valid_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 30),
            release_reason=None,
        )

        # Valid open period
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [
            valid_open_period,
            valid_incarceration_period_1,
            valid_incarceration_period_2,
            valid_incarceration_period_3,
        ]

        expected_output = [
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                external_id="3",
                state_code="US_XX",
                admission_date=date(2011, 11, 20),
                admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
                release_date=date(2012, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            ),
            valid_incarceration_period_2,
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1113,
                external_id="5",
                state_code="US_XX",
                admission_date=date(2012, 12, 24),
                admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=date(2012, 12, 30),
                release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
            ),
            valid_open_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_only_open(self) -> None:
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [valid_open_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual(incarceration_periods, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_invalid_dates(self) -> None:
        # We drop any periods with a release_date that precedes the admission_date
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [valid_open_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual([], updated_periods)

    @freeze_time("2000-01-01 00:00:00-05:00")
    def test_sort_and_infer_missing_dates_and_statuses_invalid_admission_date_in_future(
        self,
    ) -> None:
        # We drop any periods with an admission_date in the future
        invalid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [invalid_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual([], updated_periods)

    @freeze_time("2000-01-01 00:00:00-05:00")
    def test_sort_and_infer_missing_dates_and_statuses_invalid_release_date_in_future(
        self,
    ) -> None:
        # We clear the release information for release_dates in the future
        invalid_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(1990, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [invalid_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(1990, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=None,
            release_reason=None,
        )

        self.assertEqual([updated_period], updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_open_with_release_reason(
        self,
    ) -> None:
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=None,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [valid_open_period]

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        updated_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="5",
            state_code="US_XX",
            admission_date=date(2015, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertEqual([updated_period], updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_valid_open_admission(
        self,
    ) -> None:
        valid_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        # Invalid open period with same admission date as valid_incarceration_period_1
        valid_open_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1110,
            external_id="0",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incarceration_periods = [valid_incarceration_period_1, valid_open_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )
            self.assertEqual(
                incarceration_periods,
                updated_periods,
            )

    def test_sort_and_infer_missing_dates_and_statuses_multiple_open_periods(
        self,
    ) -> None:
        open_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        open_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        open_incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2001, 6, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incarceration_periods = [
            open_incarceration_period_1,
            open_incarceration_period_2,
            open_incarceration_period_3,
        ]

        # Tests that only one period is left open
        for ip_order_combo in permutations(incarceration_periods.copy()):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            num_open_periods = len(
                [ip for ip in updated_periods if ip.release_date is None]
            )

            self.assertEqual(1, num_open_periods)

    def test_sort_and_infer_missing_dates_and_statuses_no_periods(self) -> None:
        incarceration_periods: List[StateIncarcerationPeriod] = []

        updated_periods = self._sort_and_infer_missing_dates_and_statuses(
            incarceration_periods=incarceration_periods
        )

        self.assertEqual([], updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_same_dates_sort_by_transfer_start(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2008, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_period_copy = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2008, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [incarceration_period, incarceration_period_copy]

        updated_order = [incarceration_period]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(updated_order, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_same_admission_dates_sort_by_external_ids(
        self,
    ) -> None:
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2004, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        incarceration_periods = [
            second_incarceration_period,
            first_incarceration_period,
        ]

        updated_first_incarceration_period = attr.evolve(
            first_incarceration_period,
            release_date=date(2004, 1, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        expected_output = [
            updated_first_incarceration_period,
            second_incarceration_period,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_nested_period(self) -> None:
        outer_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        nested_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 3, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [outer_ip, nested_ip]

        expected_output = [outer_ip]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_multiple_nested_periods(
        self,
    ) -> None:
        outer_ip = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        nested_ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 2, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 2, 18),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="3",
            incarceration_period_id=3333,
            state_code="US_XX",
            admission_date=date(2002, 2, 18),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 6, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_ip_3 = StateIncarcerationPeriod.new_with_defaults(
            external_id="4",
            incarceration_period_id=4444,
            state_code="US_XX",
            admission_date=date(2002, 6, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 6, 29),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        nested_ip_4 = StateIncarcerationPeriod.new_with_defaults(
            external_id="5",
            incarceration_period_id=5555,
            state_code="US_XX",
            admission_date=date(2002, 6, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [
            outer_ip,
            nested_ip_1,
            nested_ip_2,
            nested_ip_3,
            nested_ip_4,
        ]

        expected_output = [outer_ip]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_partial_overlap_period(
        self,
    ) -> None:
        ip_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id="1",
            incarceration_period_id=1111,
            state_code="US_XX",
            admission_date=date(2002, 2, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2002, 9, 11),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        ip_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id="2",
            incarceration_period_id=2222,
            state_code="US_XX",
            admission_date=date(2002, 3, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2002, 10, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_periods = [ip_1, ip_2]

        expected_output = [ip_1, ip_2]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(expected_output, updated_periods)

    def test_sort_and_infer_missing_dates_and_statuses_borders_edges(self) -> None:
        zero_day_period_start = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        zero_day_period_end = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        incarceration_periods = [
            zero_day_period_start,
            valid_incarceration_period,
            zero_day_period_end,
        ]

        for ip_order_combo in permutations(incarceration_periods):
            ips_for_test = [attr.evolve(ip) for ip in ip_order_combo]

            updated_periods = self._sort_and_infer_missing_dates_and_statuses(
                incarceration_periods=ips_for_test
            )

            self.assertEqual(
                [
                    zero_day_period_start,
                    valid_incarceration_period,
                    zero_day_period_end,
                ],
                updated_periods,
            )


class TestIsZeroDayErroneousPeriod(unittest.TestCase):
    """Tests the _is_zero_day_erroneous_period function on the
    IncarcerationNormalizationManager."""

    def test_drop_zero_day_erroneous_periods(self) -> None:
        state_code = "US_XX"
        invalid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        self.assertTrue(
            IncarcerationPeriodNormalizationManager._is_zero_day_erroneous_period(
                invalid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_valid(self) -> None:
        state_code = "US_XX"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            # Don't drop a period if admission_date != release_date
            release_date=date(2008, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        self.assertFalse(
            IncarcerationPeriodNormalizationManager._is_zero_day_erroneous_period(
                valid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_transfer_adm(self) -> None:
        state_code = "US_XX"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        self.assertFalse(
            IncarcerationPeriodNormalizationManager._is_zero_day_erroneous_period(
                valid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_non_erroneous_admission(self) -> None:
        state_code = "US_XX"
        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="1",
            state_code=state_code,
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2008, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        self.assertFalse(
            IncarcerationPeriodNormalizationManager._is_zero_day_erroneous_period(
                valid_incarceration_period, None, None
            )
        )

    def test_drop_zero_day_erroneous_periods_borders_edges(self) -> None:
        zero_day_period_start = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2011, 11, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        valid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code="US_XX",
            admission_date=date(2011, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        zero_day_period_end = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2012, 12, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        self.assertTrue(
            IncarcerationPeriodNormalizationManager._is_zero_day_erroneous_period(
                zero_day_period_start, None, valid_incarceration_period
            )
        )

        self.assertTrue(
            IncarcerationPeriodNormalizationManager._is_zero_day_erroneous_period(
                zero_day_period_end, valid_incarceration_period, None
            )
        )

        self.assertFalse(
            IncarcerationPeriodNormalizationManager._is_zero_day_erroneous_period(
                valid_incarceration_period,
                zero_day_period_start,
                zero_day_period_end,
            )
        )


class TestStatusChangeEdges(unittest.TestCase):
    """Tests the various functionality related to setting accurate STATUS_CHANGE
    edges on periods."""

    def test_update_transfers_to_status_changes(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPeriodNormalizationManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        expected_updated_periods = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        self.assertEqual(expected_updated_periods, updated_periods)

    def test_update_transfers_to_status_changes_diff_custodial_authority(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPeriodNormalizationManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        expected_updated_periods = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        self.assertEqual(expected_updated_periods, updated_periods)

    def test_update_transfers_to_status_changes_not_status_change(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
            ),
            attr.evolve(
                incarceration_period_2,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPeriodNormalizationManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_update_transfers_to_status_changes_null_to_set(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        expected_updated_periods = [
            attr.evolve(
                incarceration_period_1,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
            ),
            attr.evolve(
                incarceration_period_2,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPeriodNormalizationManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        self.assertEqual(expected_updated_periods, updated_periods)

    def test_update_transfers_to_status_changes_both_null(self) -> None:
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=None,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        ip_periods_copied = [
            attr.evolve(
                incarceration_period_1,
            ),
            attr.evolve(
                incarceration_period_2,
            ),
        ]

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        updated_periods = (
            IncarcerationPeriodNormalizationManager._update_transfers_to_status_changes(
                incarceration_periods
            )
        )

        self.assertEqual(ip_periods_copied, updated_periods)

    def test_is_status_change_edge_valid_attributes(self) -> None:
        """Tests that all attributes listed in ATTRIBUTES_TRIGGERING_STATUS_CHANGE
        are valid attributes on the StateIncarcerationPeriod class."""
        for attribute in ATTRIBUTES_TRIGGERING_STATUS_CHANGE:
            if attribute not in StateIncarcerationPeriod.__dict__.get(
                "__annotations__", []
            ):
                raise ValueError(
                    "Attribute in ATTRIBUTES_TRIGGERING_STATUS_CHANGE "
                    "not valid attribute on the StateIncarcerationPeriod "
                    "class."
                )


class TestValidateIpInvariants(unittest.TestCase):
    """Tests the validate_ip_invariants function."""

    def setUp(self) -> None:
        self.valid_ip = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="3",
            state_code="US_XX",
            admission_date=date(2010, 1, 13),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

    def test_validate_ip_invariants_valid(self) -> None:
        # Assert no error
        IncarcerationPeriodNormalizationManager.validate_ip_invariants([self.valid_ip])

    def test_validate_ip_invariants_missing_admission_reason(self) -> None:
        invalid_ip_missing = attr.evolve(self.valid_ip, admission_reason=None)

        with self.assertRaises(ValueError):
            IncarcerationPeriodNormalizationManager.validate_ip_invariants(
                [invalid_ip_missing]
            )

    def test_validate_ip_invariants_missing_pfi(self) -> None:
        invalid_ip_missing = attr.evolve(
            self.valid_ip, specialized_purpose_for_incarceration=None
        )

        with self.assertRaises(ValueError):
            IncarcerationPeriodNormalizationManager.validate_ip_invariants(
                [invalid_ip_missing]
            )

    def test_validate_ip_invariants_invalid_admission_reason(self) -> None:
        invalid_ip_missing = attr.evolve(
            self.valid_ip,
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
        )

        with self.assertRaises(ValueError):
            IncarcerationPeriodNormalizationManager.validate_ip_invariants(
                [invalid_ip_missing]
            )
