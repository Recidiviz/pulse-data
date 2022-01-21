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
"""Tests the entity_normalization_utils file."""
import datetime
import unittest
from unittest import mock

from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_utils import (
    entity_normalization_managers_for_calculations,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_normalization_delegate import (
    UsXxIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_period_normalization_delegate import (
    UsXxSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


class TestIncarcerationNormalizationDelegate(UsXxIncarcerationNormalizationDelegate):
    def normalization_relies_on_supervision_periods(self) -> bool:
        return True

    def normalization_relies_on_violation_responses(self) -> bool:
        return True


class TestSupervisionNormalizationDelegate(UsXxSupervisionNormalizationDelegate):
    def normalization_relies_on_sentences(self) -> bool:
        return True


class TestNormalizationManagersForCalculations(unittest.TestCase):
    """Tests the entity_normalization_managers_for_calculations function."""

    def setUp(self) -> None:
        self.incarceration_normalization_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_utils"
            ".get_state_specific_incarceration_period_normalization_delegate"
        )
        self.mock_incarceration_normalization_delegate = (
            self.incarceration_normalization_delegate_patcher.start()
        )
        self.mock_incarceration_normalization_delegate.return_value = (
            UsXxIncarcerationNormalizationDelegate()
        )
        self.supervision_normalization_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_utils"
            ".get_state_specific_supervision_period_normalization_delegate"
        )
        self.normalization_incarceration_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_utils"
            ".get_state_specific_incarceration_delegate"
        )
        self.mock_incarceration_delegate = (
            self.normalization_incarceration_delegate_patcher.start()
        )
        self.mock_incarceration_delegate.return_value = UsXxIncarcerationDelegate()
        self.mock_supervision_normalization_delegate = (
            self.supervision_normalization_delegate_patcher.start()
        )
        self.mock_supervision_normalization_delegate.return_value = (
            UsXxSupervisionNormalizationDelegate()
        )
        self.field_index = CoreEntityFieldIndex()

    def tearDown(self) -> None:
        self.incarceration_normalization_delegate_patcher.stop()
        self.supervision_normalization_delegate_patcher.stop()
        self.normalization_incarceration_delegate_patcher.stop()

    def test_normalization_managers_for_calculations(self) -> None:
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
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        (
            ip_normalization_manager,
            sp_normalization_manager,
        ) = entity_normalization_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            normalized_violation_responses=None,
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        assert ip_normalization_manager is not None
        self.assertEqual(
            [incarceration_period],
            ip_normalization_manager.normalized_incarceration_period_index_for_calculations(
                collapse_transfers=False,
                overwrite_facility_information_in_transfers=False,
            ).incarceration_periods,
        )
        assert sp_normalization_manager is not None
        self.assertEqual(
            [supervision_period],
            sp_normalization_manager.normalized_supervision_period_index_for_calculations().supervision_periods,
        )

    def test_normalization_managers_for_calculations_no_sps(self) -> None:
        state_code = "US_XX"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=datetime.date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=datetime.date(2020, 5, 17),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        (
            ip_normalization_manager,
            sp_normalization_manager,
        ) = entity_normalization_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=[incarceration_period],
            supervision_periods=None,
            normalized_violation_responses=None,
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        assert ip_normalization_manager is not None
        self.assertEqual(
            [incarceration_period],
            ip_normalization_manager.normalized_incarceration_period_index_for_calculations(
                collapse_transfers=False,
                overwrite_facility_information_in_transfers=False,
            ).incarceration_periods,
        )
        self.assertIsNone(sp_normalization_manager)

    def test_normalization_managers_for_calculations_no_sps_state_requires(
        self,
    ) -> None:
        self.mock_incarceration_normalization_delegate.return_value = (
            TestIncarcerationNormalizationDelegate()
        )
        state_code = "US_XX"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
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
            (_, _,) = entity_normalization_managers_for_calculations(
                state_code=state_code,
                incarceration_periods=[incarceration_period],
                supervision_periods=None,
                normalized_violation_responses=[],
                field_index=self.field_index,
                incarceration_sentences=None,
                supervision_sentences=None,
            )

    def test_normalization_managers_for_calculations_no_violation_responses_state_requires(
        self,
    ) -> None:
        self.mock_incarceration_normalization_delegate.return_value = (
            TestIncarcerationNormalizationDelegate()
        )
        state_code = "US_XX"
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
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
            (_, _,) = entity_normalization_managers_for_calculations(
                state_code=state_code,
                incarceration_periods=[incarceration_period],
                supervision_periods=[],
                normalized_violation_responses=None,
                field_index=self.field_index,
                incarceration_sentences=None,
                supervision_sentences=None,
            )

    def test_normalization_managers_for_calculations_no_sentences_state_requires(
        self,
    ) -> None:
        self.mock_supervision_normalization_delegate.return_value = (
            TestSupervisionNormalizationDelegate()
        )
        state_code = "US_XX"
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            state_code="US_XX",
            start_date=datetime.date(2017, 5, 1),
            termination_date=datetime.date(2018, 2, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
        )

        with self.assertRaises(ValueError):
            # Assert an error is raised if either sentences arg is None for a state
            # that relies on sentences for SP pre-processing
            (_, _) = entity_normalization_managers_for_calculations(
                state_code=state_code,
                incarceration_periods=[],
                supervision_periods=[supervision_period],
                normalized_violation_responses=None,
                field_index=self.field_index,
                incarceration_sentences=None,
                supervision_sentences=None,
            )

    def test_normalization_managers_for_calculations_no_ips(self) -> None:
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
            ip_normalization_manager,
            sp_normalization_manager,
        ) = entity_normalization_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=None,
            supervision_periods=[supervision_period],
            normalized_violation_responses=None,
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        self.assertIsNone(ip_normalization_manager)
        assert sp_normalization_manager is not None
        self.assertEqual(
            [supervision_period],
            sp_normalization_manager.normalized_supervision_period_index_for_calculations().supervision_periods,
        )

    def test_normalization_managers_for_calculations_empty_lists(self) -> None:
        state_code = "US_XX"

        (
            ip_normalization_manager,
            sp_normalization_manager,
        ) = entity_normalization_managers_for_calculations(
            state_code=state_code,
            incarceration_periods=[],
            supervision_periods=[],
            normalized_violation_responses=[],
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        assert ip_normalization_manager is not None
        assert sp_normalization_manager is not None
        self.assertEqual([], ip_normalization_manager._original_incarceration_periods)
        self.assertEqual([], sp_normalization_manager._supervision_periods)
