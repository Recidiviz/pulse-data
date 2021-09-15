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
"""Tests the entity_pre_processing_utils file."""
import datetime
import unittest
from unittest import mock

from recidiviz.calculator.pipeline.utils.entity_pre_processing_utils import (
    pre_processing_managers_for_calculations,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_incarceration_period_pre_processing_delegate import (
    UsXxIncarcerationPreProcessingDelegate,
)


class TestIncarcerationPreProcessingDelegate(UsXxIncarcerationPreProcessingDelegate):
    def pre_processing_relies_on_supervision_periods(self) -> bool:
        return True

    def pre_processing_relies_on_violation_responses(self) -> bool:
        return True


class TestPreProcessingManagersForCalculations(unittest.TestCase):
    """Tests the pre_processing_managers_for_calculations function."""

    def setUp(self) -> None:
        self.pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils"
            ".get_state_specific_incarceration_period_pre_processing_delegate"
        )
        self.mock_pre_processing_delegate = self.pre_processing_delegate_patcher.start()
        self.mock_pre_processing_delegate.return_value = (
            UsXxIncarcerationPreProcessingDelegate()
        )

    def tearDown(self) -> None:
        self.pre_processing_delegate_patcher.stop()

    def test_pre_processing_managers_for_calculations(self) -> None:
        state_code = "US_XX"
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=datetime.date(2017, 3, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        (
            ip_pre_processing_manager,
            sp_pre_processing_manager,
        ) = pre_processing_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            pre_processed_violation_responses=None,
        )

        assert ip_pre_processing_manager is not None
        self.assertEqual(
            [incarceration_period],
            ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
                collapse_transfers=False,
                overwrite_facility_information_in_transfers=False,
            ).incarceration_periods,
        )
        assert sp_pre_processing_manager is not None
        self.assertEqual(
            [supervision_period],
            sp_pre_processing_manager.pre_processed_supervision_period_index_for_calculations().supervision_periods,
        )

    def test_pre_processing_managers_for_calculations_no_sps(self) -> None:
        state_code = "US_XX"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        (
            ip_pre_processing_manager,
            sp_pre_processing_manager,
        ) = pre_processing_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=[incarceration_period],
            supervision_periods=None,
            pre_processed_violation_responses=None,
        )

        assert ip_pre_processing_manager is not None
        self.assertEqual(
            [incarceration_period],
            ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
                collapse_transfers=False,
                overwrite_facility_information_in_transfers=False,
            ).incarceration_periods,
        )
        self.assertIsNone(sp_pre_processing_manager)

    def test_pre_processing_managers_for_calculations_no_sps_state_requires(
        self,
    ) -> None:
        self.mock_pre_processing_delegate.return_value = (
            TestIncarcerationPreProcessingDelegate()
        )
        state_code = "US_XX"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        with self.assertRaises(ValueError):
            # Assert an error is raised if the supervision_periods arg is None for a
            # state that relies on supervision periods for IP pre-processing
            (_, _,) = pre_processing_managers_for_calculations(
                state_code=state_code,
                incarceration_periods=[incarceration_period],
                supervision_periods=None,
                pre_processed_violation_responses=[],
            )

    def test_pre_processing_managers_for_calculations_no_violation_responses_state_requires(
        self,
    ) -> None:
        self.mock_pre_processing_delegate.return_value = (
            TestIncarcerationPreProcessingDelegate()
        )
        state_code = "US_XX"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        with self.assertRaises(ValueError):
            # Assert an error is raised if the violation_responses arg is None for a
            # state that relies on violation responses for IP pre-processing
            (_, _,) = pre_processing_managers_for_calculations(
                state_code=state_code,
                incarceration_periods=[incarceration_period],
                supervision_periods=[],
                pre_processed_violation_responses=None,
            )

    def test_pre_processing_managers_for_calculations_no_ips(self) -> None:
        state_code = "US_XX"
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=datetime.date(2017, 3, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        (
            ip_pre_processing_manager,
            sp_pre_processing_manager,
        ) = pre_processing_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=None,
            supervision_periods=[supervision_period],
            pre_processed_violation_responses=None,
        )

        self.assertIsNone(ip_pre_processing_manager)
        assert sp_pre_processing_manager is not None
        self.assertEqual(
            [supervision_period],
            sp_pre_processing_manager.pre_processed_supervision_period_index_for_calculations().supervision_periods,
        )

    def test_pre_processing_managers_for_calculations_empty_lists(self) -> None:
        state_code = "US_XX"

        (
            ip_pre_processing_manager,
            sp_pre_processing_manager,
        ) = pre_processing_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=[],
            supervision_periods=[],
            pre_processed_violation_responses=[],
        )

        assert ip_pre_processing_manager is not None
        assert sp_pre_processing_manager is not None
        self.assertEqual([], ip_pre_processing_manager._incarceration_periods)
        self.assertEqual([], sp_pre_processing_manager._supervision_periods)
