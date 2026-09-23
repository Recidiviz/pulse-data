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
from recidiviz.persistence.entity.activity.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_supervision_period_normalization_delegate import (
    UsMiSupervisionNormalizationDelegate,
)

_STATE_CODE = StateCode.US_TN.value
_PERSON_ID = 12312345


class TestUsMiSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsMiSupervisionNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsMiSupervisionNormalizationDelegate()

    # This tests that normalization will drop INVESTIGATION
    def test_drop_investigation(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )
        self.assertEqual(
            [],
            self.delegate.drop_bad_periods([supervision_period]),
        )

    # This tests that normalization will convert supervision level to IN_CUSTODY if supervision level is null and the period doesn't end in discharge
    def test_supervision_level_override(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=None,
        )

        self.assertEqual(
            StateSupervisionLevel.IN_CUSTODY,
            self.delegate.supervision_level_override(0, [supervision_period]),
        )

    # This tests that normalization will not convert supervision level to IN_CUSTODY if supervision level is null but the period ends in discharge
    def test_no_supervision_level_override(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=None,
        )

        self.assertEqual(
            None,
            self.delegate.supervision_level_override(0, [supervision_period]),
        )

    def test_infer_additional_periods_no_incarceration_periods(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        self.assertEqual(
            [supervision_period],
            self.delegate.infer_additional_periods(
                _PERSON_ID, [supervision_period], []
            ),
        )

    def test_infer_additional_periods_full_overlap_with_open_periods(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            state_code=_STATE_CODE,
            external_id="ip-1",
            admission_date=date(2023, 6, 1),
            release_date=None,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_inferred_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=4722536687347008883,
            state_code=_STATE_CODE,
            external_id="sp-1-0-IN-CUSTODY",
            start_date=date(2023, 6, 1),
            termination_date=None,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="INFERRED_FROM_INCARCERATION_ADMISSION_REASON_SANCTION_ADMISSION",
        )

        self.assertEqual(
            [supervision_period, expected_inferred_period],
            self.delegate.infer_additional_periods(
                _PERSON_ID, [supervision_period], [incarceration_period]
            ),
        )

    def test_infer_additional_periods_partial_overlap_in_middle_of_closed_period(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 12, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2,
            state_code=_STATE_CODE,
            external_id="ip-2",
            admission_date=date(2023, 6, 1),
            release_date=date(2023, 8, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        )

        expected_inferred_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=4743214210537916298,
            state_code=_STATE_CODE,
            external_id="sp-2-0-IN-CUSTODY",
            start_date=date(2023, 6, 1),
            termination_date=date(2023, 8, 1),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="INFERRED_FROM_INCARCERATION_ADMISSION_REASON_REVOCATION",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="INFERRED_FROM_INCARCERATION_RELEASE_REASON_RELEASED_TO_SUPERVISION",
        )

        self.assertEqual(
            [supervision_period, expected_inferred_period],
            self.delegate.infer_additional_periods(
                _PERSON_ID, [supervision_period], [incarceration_period]
            ),
        )

    def test_infer_additional_periods_partial_overlap_at_start(self) -> None:
        """The incarceration period starts before the supervision period, so the
        inferred period should be clipped to the supervision period's own start
        date, not the (earlier) incarceration admission date."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=8,
            state_code=_STATE_CODE,
            external_id="sp-7",
            start_date=date(2023, 3, 1),
            termination_date=date(2023, 12, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=8,
            state_code=_STATE_CODE,
            external_id="ip-7",
            admission_date=date(2023, 1, 1),
            release_date=date(2023, 5, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        )

        expected_inferred_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=4747639109391527069,
            state_code=_STATE_CODE,
            external_id="sp-7-0-IN-CUSTODY",
            start_date=date(2023, 3, 1),
            termination_date=date(2023, 5, 1),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="INFERRED_FROM_INCARCERATION_ADMISSION_REASON_REVOCATION",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="INFERRED_FROM_INCARCERATION_RELEASE_REASON_RELEASED_TO_SUPERVISION",
        )

        self.assertEqual(
            [supervision_period, expected_inferred_period],
            self.delegate.infer_additional_periods(
                _PERSON_ID, [supervision_period], [incarceration_period]
            ),
        )

    def test_infer_additional_periods_partial_overlap_at_end(self) -> None:
        """The incarceration period is still open past the supervision period's own
        termination date, so the inferred period should be clipped to the
        supervision period's end, and the termination reason raw text should
        reflect that the incarceration period has not actually released yet."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=9,
            state_code=_STATE_CODE,
            external_id="sp-8",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 6, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=9,
            state_code=_STATE_CODE,
            external_id="ip-8",
            admission_date=date(2023, 4, 1),
            release_date=None,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        expected_inferred_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=4753001449722611668,
            state_code=_STATE_CODE,
            external_id="sp-8-0-IN-CUSTODY",
            start_date=date(2023, 4, 1),
            termination_date=date(2023, 6, 1),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="INFERRED_FROM_INCARCERATION_ADMISSION_REASON_SANCTION_ADMISSION",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="INFERRED_FROM_INCARCERATION_RELEASE_REASON_UNKNOWN",
        )

        self.assertEqual(
            [supervision_period, expected_inferred_period],
            self.delegate.infer_additional_periods(
                _PERSON_ID, [supervision_period], [incarceration_period]
            ),
        )

    def test_infer_additional_periods_multiple_overlapping_incarceration_periods(
        self,
    ) -> None:
        """Two separate, non-contiguous incarceration periods overlapping the same
        open supervision period should produce two separate inferred periods with
        distinct ids, rather than being merged or colliding."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=6,
            state_code=_STATE_CODE,
            external_id="sp-6",
            start_date=date(2023, 1, 1),
            termination_date=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=6,
            state_code=_STATE_CODE,
            external_id="ip-6a",
            admission_date=date(2023, 3, 1),
            release_date=date(2023, 4, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        )
        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=7,
            state_code=_STATE_CODE,
            external_id="ip-6b",
            admission_date=date(2023, 7, 1),
            release_date=date(2023, 8, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
        )

        expected_first_inferred_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=4764572759899258677,
            state_code=_STATE_CODE,
            external_id="sp-6-0-IN-CUSTODY",
            start_date=date(2023, 3, 1),
            termination_date=date(2023, 4, 1),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="INFERRED_FROM_INCARCERATION_ADMISSION_REASON_SANCTION_ADMISSION",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="INFERRED_FROM_INCARCERATION_RELEASE_REASON_RELEASED_TO_SUPERVISION",
        )
        expected_second_inferred_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=4711406056507968727,
            state_code=_STATE_CODE,
            external_id="sp-6-1-IN-CUSTODY",
            start_date=date(2023, 7, 1),
            termination_date=date(2023, 8, 1),
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            admission_reason_raw_text="INFERRED_FROM_INCARCERATION_ADMISSION_REASON_SANCTION_ADMISSION",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            termination_reason_raw_text="INFERRED_FROM_INCARCERATION_RELEASE_REASON_RELEASED_TO_SUPERVISION",
        )

        result = self.delegate.infer_additional_periods(
            _PERSON_ID,
            [supervision_period],
            [first_incarceration_period, second_incarceration_period],
        )

        self.assertEqual(
            [
                supervision_period,
                expected_first_inferred_period,
                expected_second_inferred_period,
            ],
            result,
        )
        self.assertNotEqual(
            result[1].supervision_period_id, result[2].supervision_period_id
        )

    def test_infer_additional_periods_skips_already_in_custody(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=5,
            state_code=_STATE_CODE,
            external_id="sp-5",
            start_date=date(2023, 1, 1),
            termination_date=None,
            supervision_level=StateSupervisionLevel.IN_CUSTODY,
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=5,
            state_code=_STATE_CODE,
            external_id="ip-5",
            admission_date=date(2023, 6, 1),
            release_date=None,
        )

        self.assertEqual(
            [supervision_period],
            self.delegate.infer_additional_periods(
                _PERSON_ID, [supervision_period], [incarceration_period]
            ),
        )
