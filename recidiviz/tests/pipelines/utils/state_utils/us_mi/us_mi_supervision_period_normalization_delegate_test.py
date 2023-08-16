#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests us_mi_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_supervision_period_normalization_delegate import (
    UsMiSupervisionNormalizationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsMiSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsMiSupervisionNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsMiSupervisionNormalizationDelegate()

    # This tests that when there is an open period that starts on a discharge date and
    # isn't starting because of a new movement our court sentence, and that's the latest
    # period for this person, normalization will drop that period
    def test_drop_last_open_period(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 7, 1),
            termination_date=None,
            admission_reason_raw_text=None,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        self.assertEqual(
            [supervision_period],
            self.delegate.drop_bad_periods([supervision_period, supervision_period_2]),
        )

    # This tests that when there is an open period that starts on a discharge date and
    # it's starting because of a movement, normalization will not drop any periods
    def test_dont_when_new_movement_period(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 7, 1),
            termination_date=None,
            admission_reason_raw_text="2",
            admission_reason=StateSupervisionPeriodAdmissionReason.INVESTIGATION,
        )

        self.assertEqual(
            [supervision_period, supervision_period_2],
            self.delegate.drop_bad_periods([supervision_period, supervision_period_2]),
        )

    # This tests that when there are multiple open periods that start on that
    # start on a discharge date, normalization period will not drop any periods
    def test_drop_when_multiple_bad_open_periods(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 7, 1),
            termination_date=None,
            admission_reason_raw_text=None,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )
        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-3",
            start_date=date(2023, 7, 1),
            termination_date=None,
            admission_reason_raw_text=None,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        self.assertEqual(
            [supervision_period, supervision_period_2, supervision_period_3],
            self.delegate.drop_bad_periods(
                [supervision_period, supervision_period_2, supervision_period_3]
            ),
        )

    # This tests that when there is an open period that starts after a discharge date,
    # normalization will not drop that period
    def test_drop_when_open_period_starts_later(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 7, 2),
            termination_date=None,
            admission_reason_raw_text=None,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        self.assertEqual(
            [supervision_period, supervision_period_2],
            self.delegate.drop_bad_periods([supervision_period, supervision_period_2]),
        )

    # This tests that when there is an closed period that starts on a discharge date,
    # normalization will not drop that period
    def test_drop_when_closed_period_starts_on_discharge(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 7, 1),
            termination_date=date(2023, 7, 5),
            admission_reason_raw_text=None,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        self.assertEqual(
            [supervision_period, supervision_period_2],
            self.delegate.drop_bad_periods([supervision_period, supervision_period_2]),
        )

    # This tests that normalization will only drop the open period if the external id
    # indicates that it was considered by ingest to be the "last" supervision period
    # for one run (to ensure we only drop entity deletion issues)
    def test_drop_when_periods_misordered(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 7, 1),
            termination_date=None,
            admission_reason_raw_text=None,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )
        self.assertEqual(
            [supervision_period, supervision_period_2],
            self.delegate.drop_bad_periods([supervision_period, supervision_period_2]),
        )

    # This tests that normalization will not drop any periods when there's just one period
    def test_drop_one_period(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        self.assertEqual(
            [supervision_period],
            self.delegate.drop_bad_periods([supervision_period]),
        )
