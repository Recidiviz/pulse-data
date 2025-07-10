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
"""Tests us_tn_violation_response_normalization_delegate.py."""
import unittest
from datetime import date
from typing import List
from unittest import mock

from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_violation_response_normalization_delegate import (
    UsTnViolationResponseNormalizationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsTnViolationResponseNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsTnViolationResponseNormalizationDelegate."""

    def setUp(self) -> None:
        self.person_id = 4700000000000000123

        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils.generate_primary_key"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 4700000000012312345

    def tearDown(self) -> None:
        self.unique_id_patcher.stop()

    @staticmethod
    def _build_delegate(
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> UsTnViolationResponseNormalizationDelegate:
        return UsTnViolationResponseNormalizationDelegate(
            incarceration_periods=incarceration_periods
        )

    # Test case 1: there's an inferred violation with a VIOLT admission incarceration period following the response with no other conflicting violations
    def test_get_additional_violation_types_for_response_single_technical_violation_type_from_violt(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-INFERRED-2023-05-01 14:30:00",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-INFERRED-2023-11-02 14:30:00",
            response_date=date(2022, 11, 2),
        )

        next_next_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="00000001-INFERRED-2023-03-01 14:30:00",
                response_date=date(2023, 3, 1),
            )
        )

        violation_responses = [
            violation_response,
            next_violation_response,
            next_next_violation_response,
        ]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="PAFA-VIOLT",
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2023, 6, 1),
            admission_reason_raw_text="PAFA_VIOLW",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(len(result), 1)

        self.assertEqual(
            (result[0]).violation_type, StateSupervisionViolationType.TECHNICAL
        )

    # Test case 2: there's an inferred violation with a VIOLT admission incarceration period following the a VRPT response but also a RFRS violation after that violation and before the IP start, so we should not infer that the VIOLT was a technical
    def test_get_additional_violation_types_for_response_not_inferred_technical_due_to_documented_violation_before_ip(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-INFERRED-2023-05-01 14:30:00",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-1234567-1",
            response_date=date(2022, 6, 1),
        )

        next_next_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="00000001-INFERRED-2023-03-01 14:30:00",
                response_date=date(2023, 3, 1),
            )
        )

        violation_responses = [
            violation_response,
            next_violation_response,
            next_next_violation_response,
        ]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 7, 1),
            admission_reason_raw_text="PAFA-VIOLT",
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2023, 4, 1),
            admission_reason_raw_text="PAFA_VIOLW",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(0, len(result))

    # Test case 2: there's an inferred violation and there is a VIOLT admission incarceration period but it is more than 6 months after the inferred violation is documented, so we don't infer technical
    def test_get_additional_violation_types_for_response_not_inferred_more_than_6_months_after(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-INFERRED-2023-05-01 14:30:00",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-1234567-1",
            response_date=date(2022, 6, 1),
        )

        next_next_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="00000001-INFERRED-2023-03-01 14:30:00",
                response_date=date(2023, 3, 1),
            )
        )

        violation_responses = [
            violation_response,
            next_violation_response,
            next_next_violation_response,
        ]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2023, 7, 1),
            admission_reason_raw_text="PAFA-VIOLT",
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2023, 4, 1),
            admission_reason_raw_text="PAFA_VIOLW",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(0, len(result))

    # Test case 3: there's an inferred violation with a VIOLW admission incarceration period and an inferred violation before that should be of type LAW. Then 10 months later, a VIOLT with an inferred 1 month before that should be a TECHNICAL.
    def test_get_additional_violation_types_for_response_inferred_law_then_technical(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-INFERRED-2023-05-01 14:30:00",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="00000001-1234567-1",
            response_date=date(2022, 12, 1),
        )

        next_next_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="00000001-INFERRED-2023-03-01 14:30:00",
                response_date=date(2023, 3, 1),
            )
        )

        violation_responses = [
            violation_response,
            next_violation_response,
            next_next_violation_response,
        ]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 7, 1),
            admission_reason_raw_text="PAFA-VIOLW",
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2023, 4, 1),
            admission_reason_raw_text="PAFA_VIOLT",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        # first confirm we see the first violation_type for the VIOLW
        result_first = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(1, len(result_first))

        self.assertEqual(
            StateSupervisionViolationType.LAW, (result_first[0]).violation_type
        )

        # then confirm we don't have a violation type added for the non-INFERRED violation
        result_second = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, next_violation_response, 0, violation_responses
        )

        self.assertEqual(0, len(result_second))

        # finally add violation_type TECHINCAL for later INFERRED violation
        result_third = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, next_next_violation_response, 2, violation_responses
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, (result_third[0]).violation_type
        )
