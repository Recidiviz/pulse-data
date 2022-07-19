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
"""Tests for the us_pa_supervision_period_normalization_delegate.py file"""
import unittest
from datetime import date

import mock

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    clear_entity_id_index_cache,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_period_normalization_delegate import (
    UsPaSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


class TestUsPaSupervisionNormalizationDelegate(unittest.TestCase):
    """Unit tests for UsPaSupervisionPreProcdessingDelegate"""

    def setUp(self) -> None:
        self.delegate = UsPaSupervisionNormalizationDelegate()
        self.person_id = 4200000123

        clear_entity_id_index_cache()
        self.unique_id_patcher = mock.patch(
            "recidiviz.calculator.pipeline.normalization.utils."
            "normalized_entities_utils._fixed_length_object_id_for_entity"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 12345

    def test_infer_additional_periods(self) -> None:
        supervision_period_absconsion = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code=StateCode.US_PA.value,
            start_date=date(2010, 2, 1),
            termination_date=date(2010, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_site="XXX",
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        supervision_period_non_absconsion = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            state_code=StateCode.US_PA.value,
            start_date=date(2010, 3, 15),
            termination_date=date(2010, 4, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_site="XXX",
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        inferred_periods = self.delegate.infer_additional_periods(
            self.person_id,
            [supervision_period_absconsion, supervision_period_non_absconsion],
            [],
        )
        expected_periods = [
            supervision_period_absconsion,
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=420000012312345,
                state_code=StateCode.US_PA.value,
                start_date=date(2010, 3, 1),
                termination_date=date(2010, 3, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
                termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
                supervision_site=None,
                supervision_level=StateSupervisionLevel.MEDIUM,
            ),
            supervision_period_non_absconsion,
        ]

        self.assertEqual(expected_periods, inferred_periods)

    def test_infer_additional_periods_open_absconsion_period_if_last_one_no_subsequent_period(
        self,
    ) -> None:
        supervision_period_absconsion = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code=StateCode.US_PA.value,
            start_date=date(2010, 2, 1),
            termination_date=date(2010, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_site="XXX",
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        inferred_periods = self.delegate.infer_additional_periods(
            self.person_id, [supervision_period_absconsion], []
        )
        expected_periods = [
            supervision_period_absconsion,
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=420000012312345,
                state_code=StateCode.US_PA.value,
                start_date=date(2010, 3, 1),
                termination_date=None,
                admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
                termination_reason=None,
                supervision_site=None,
                supervision_level=StateSupervisionLevel.MEDIUM,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]
        self.assertEqual(expected_periods, inferred_periods)

    def test_infer_additional_periods_absconsion_period_ends_at_start_of_next_incarceration_period(
        self,
    ) -> None:
        supervision_period_absconsion = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code=StateCode.US_PA.value,
            start_date=date(2010, 2, 1),
            termination_date=date(2010, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_site="XXX",
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        incarceration_period_post_absconsion = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1,
                state_code=StateCode.US_PA.value,
                admission_date=date(2010, 3, 15),
                release_date=date(2010, 4, 30),
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            )
        )
        inferred_periods = self.delegate.infer_additional_periods(
            self.person_id,
            [supervision_period_absconsion],
            [incarceration_period_post_absconsion],
        )
        expected_periods = [
            supervision_period_absconsion,
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=420000012312345,
                state_code=StateCode.US_PA.value,
                start_date=date(2010, 3, 1),
                termination_date=date(2010, 3, 15),
                admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
                termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
                supervision_site=None,
                supervision_level=StateSupervisionLevel.MEDIUM,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
        ]
        self.assertEqual(expected_periods, inferred_periods)

    def test_infer_additional_periods_absconsion_period_ends_at_start_of_next_supervision_period(
        self,
    ) -> None:
        supervision_period_absconsion = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code=StateCode.US_PA.value,
            start_date=date(2010, 2, 1),
            termination_date=date(2010, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_site="XXX",
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        supervision_period_post_absconsion = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            state_code=StateCode.US_PA.value,
            start_date=date(2010, 3, 15),
            termination_date=date(2010, 3, 30),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
            supervision_site="XXX",
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        incarceration_period_post_absconsion = (
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1,
                state_code=StateCode.US_PA.value,
                admission_date=date(2010, 3, 16),
                release_date=date(2010, 4, 30),
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            )
        )
        inferred_periods = self.delegate.infer_additional_periods(
            self.person_id,
            [supervision_period_absconsion, supervision_period_post_absconsion],
            [incarceration_period_post_absconsion],
        )
        expected_periods = [
            supervision_period_absconsion,
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=420000012312345,
                state_code=StateCode.US_PA.value,
                start_date=date(2010, 3, 1),
                termination_date=date(2010, 3, 15),
                admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
                termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
                supervision_site=None,
                supervision_level=StateSupervisionLevel.MEDIUM,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            supervision_period_post_absconsion,
        ]
        self.assertEqual(expected_periods, inferred_periods)
