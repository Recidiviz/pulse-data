#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
""""Tests the us_mi_violation_response_normalization_delegate.py file."""
import unittest
from datetime import date
from typing import List
from unittest import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionViolationType,
)
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_violation_response_normalization_delegate import (
    UsMiViolationResponseNormalizationDelegate,
)

_STATE_CODE = StateCode.US_MI.value


class TestUsMiViolationResponseNormalizationDelegate(unittest.TestCase):
    """Tests the us_mi_violation_response_normalization_delegate."""

    def setUp(self) -> None:
        self.person_id = 2600000000000000123

        self.unique_id_patcher = mock.patch(
            "recidiviz.persistence.entity."
            "normalized_entities_utils.generate_primary_key"
        )
        self.mock_unique_id = self.unique_id_patcher.start()
        self.mock_unique_id.return_value = 2600000000012312345

    def tearDown(self) -> None:
        self.unique_id_patcher.stop()

    @staticmethod
    def _build_delegate(
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> UsMiViolationResponseNormalizationDelegate:
        return UsMiViolationResponseNormalizationDelegate(
            incarceration_periods=incarceration_periods
        )

    # Test case 1: there's a probation revocation incarceration period following the probation violation response
    def test_get_additional_violation_types_for_response_fill(self) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-1",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PAROLE-2",
            response_date=date(2022, 5, 2),
        )

        next_next_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="PROBATION-2",
                response_date=date(2022, 7, 1),
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
            admission_reason_raw_text="NOT14",
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="14",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(len(result), 1)

        self.assertEqual((result[0]).violation_type, StateSupervisionViolationType.LAW)

    # Test case 2: there aren't any probation revocation incarceration period following the probation violation response
    def test_get_additional_violation_types_for_response_no_revocation_ips(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-1",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-2",
            response_date=date(2022, 7, 1),
        )

        violation_responses = [violation_response, next_violation_response]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="NOT14",
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="NOT15",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(len(result), 0)

    # Test case 3: there's another probation supervision violation before the next probation revocation IP
    def test_get_additional_violation_types_for_response_next_revocation_after_another_probation_violation(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-1",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-2",
            response_date=date(2022, 5, 2),
        )

        violation_responses = [violation_response, next_violation_response]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="NOT14",
            external_id="ip-1",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="14",
            external_id="ip-2",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(len(result), 0)

    # Test case 4: response is not a probation violation
    def test_get_additional_violation_types_for_response_not_probation(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PAROLE-1",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-2",
            response_date=date(2022, 7, 1),
        )

        violation_responses = [violation_response, next_violation_response]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip1",
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="NOT14",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip2",
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="14",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(len(result), 0)

    # Test case 5: Two probation revocation IPs on the same day
    def test_get_additional_violation_types_for_response_revocations_on_same_day(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-1",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-2",
            response_date=date(2022, 7, 1),
        )

        violation_responses = [violation_response, next_violation_response]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip-2",
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="14",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip-1",
            admission_date=date(2022, 6, 1),
            admission_reason_raw_text="15",
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

    # Test case 6: Next violation is not a probation violation (so no subsequent violations)
    def test_get_additional_violation_types_for_response_no_next_probation_violation(
        self,
    ) -> None:
        prev_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-0",
            response_date=date(2022, 5, 1),
        )

        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-1",
            response_date=date(2022, 5, 1),
        )

        next_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PAROLE-2",
            response_date=date(2022, 7, 1),
        )

        violation_responses = [
            prev_violation_response,
            violation_response,
            next_violation_response,
        ]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip-2",
            admission_date=date(2022, 8, 1),
            admission_reason_raw_text="14",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip-1",
            admission_date=date(2022, 10, 1),
            admission_reason_raw_text="15",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 1, violation_responses
        )

        self.assertEqual(len(result), 1)

        self.assertEqual((result[0]).violation_type, StateSupervisionViolationType.LAW)

    # Test case 7: only 1 violation response
    def test_get_additional_violation_types_for_response_one_probation_violation(
        self,
    ) -> None:
        violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="PROBATION-1",
            response_date=date(2022, 5, 1),
        )

        violation_responses = [violation_response]

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip-2",
            admission_date=date(2022, 8, 1),
            admission_reason_raw_text="14",
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip-1",
            admission_date=date(2022, 10, 1),
            admission_reason_raw_text="15",
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        result = self._build_delegate(
            incarceration_periods
        ).get_additional_violation_types_for_response(
            0, violation_response, 0, violation_responses
        )

        self.assertEqual(len(result), 1)

        self.assertEqual((result[0]).violation_type, StateSupervisionViolationType.LAW)
