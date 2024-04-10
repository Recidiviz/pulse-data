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
"""Tests us_tn_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date

from freezegun import freeze_time

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_supervision_period_normalization_delegate import (
    UsTnSupervisionNormalizationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsTnSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsTnSupervisionNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsTnSupervisionNormalizationDelegate()

    def test_supervision_level_override_with_external_unknown_within_31_days(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 1, 31),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.MINIMUM,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 1, 31),
            termination_date=date(2023, 2, 10),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        )

        self.assertEqual(
            StateSupervisionLevel.MINIMUM,
            self.delegate.supervision_level_override(
                1,
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
            ),
        )

    def test_supervision_level_override_with_external_unknown_outside_31_days(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 1, 31),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.MINIMUM,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 1, 31),
            termination_date=date(2023, 3, 30),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        )

        self.assertEqual(
            StateSupervisionLevel.EXTERNAL_UNKNOWN,
            self.delegate.supervision_level_override(
                1,
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
            ),
        )

    def test_supervision_level_override_with_external_unknown_first_period_no_override(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 1, 31),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.MINIMUM,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 1, 31),
            termination_date=date(2023, 2, 15),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        )

        self.assertEqual(
            StateSupervisionLevel.MINIMUM,
            self.delegate.supervision_level_override(
                0,
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
            ),
        )

    def test_supervision_level_override_with_external_unknown_for_null_termination_date_outside_31_days(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 1, 31),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.MINIMUM,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 1, 31),
            termination_date=None,
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        )

        self.assertEqual(
            StateSupervisionLevel.EXTERNAL_UNKNOWN,
            self.delegate.supervision_level_override(
                1,
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
            ),
        )

    @freeze_time("2023-04-20")
    def test_supervision_level_override_with_external_unknown_for_null_termination_date_within_31_days(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 3, 31),
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.MINIMUM,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 3, 31),
            termination_date=None,
            supervision_level_raw_text="",
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        )

        self.assertEqual(
            StateSupervisionLevel.MINIMUM,
            self.delegate.supervision_level_override(
                1,
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
            ),
        )

    # ~~ Add new tests here ~~

    def test_close_incorrectly_open_supervision_periods(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 3, 31),
            termination_reason_raw_text="RNO",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 3, 31),
            termination_date=None,
            termination_reason_raw_text=None,
            termination_reason=None,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="ip1",
            admission_date=date(2023, 12, 15),
            release_date=date(2024, 1, 5),
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="ip2",
            admission_date=date(2024, 1, 5),
            release_date=date(2024, 1, 20),
            release_reason_raw_text="FADI-EXPDE",
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        supervision_period_2_corrected = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 3, 31),
            termination_date=date(2024, 1, 20),
            termination_reason_raw_text="FADI-EXPDE",
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        new_supervision_periods = [supervision_period_1, supervision_period_2_corrected]

        self.assertEqual(
            new_supervision_periods,
            self.delegate.close_incorrectly_open_supervision_periods(
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
                [
                    incarceration_period_1,
                    incarceration_period_2,
                ],
            ),
        )

    def test_close_open_supervision_periods_with_wrong_releasereason(
        # Testing case where IP doesnt end in SENTENCE_SERVED so we don't want to close SP
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 3, 31),
            termination_reason_raw_text="RNO",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 3, 31),
            termination_date=None,
            termination_reason_raw_text=None,
            termination_reason=None,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="ip1",
            admission_date=date(2023, 12, 15),
            release_date=date(2024, 1, 5),
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="ip2",
            admission_date=date(2024, 1, 5),
            release_date=date(2024, 1, 20),
            release_reason_raw_text="PAFA-PAVOK",
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(
            supervision_periods,
            self.delegate.close_incorrectly_open_supervision_periods(
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
                [
                    incarceration_period_1,
                    incarceration_period_2,
                ],
            ),
        )

    def test_close_open_supervision_periods_with_wrong_dates(
        # Testing case where IP release date doesn't come after SP start date
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=date(2023, 3, 31),
            termination_reason_raw_text="RNO",
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="sp2",
            start_date=date(2023, 3, 31),
            termination_date=None,
            termination_reason_raw_text=None,
            termination_reason=None,
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="ip1",
            admission_date=date(2020, 12, 15),
            release_date=date(2021, 1, 5),
            release_reason_raw_text="UNITMOVEMENTFH-UNIT_MOVEMENT",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            external_id="ip2",
            admission_date=date(2021, 1, 5),
            release_date=date(2022, 1, 20),
            release_reason_raw_text="FADI-EXPDE",
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(
            supervision_periods,
            self.delegate.close_incorrectly_open_supervision_periods(
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
                [
                    incarceration_period_1,
                    incarceration_period_2,
                ],
            ),
        )
