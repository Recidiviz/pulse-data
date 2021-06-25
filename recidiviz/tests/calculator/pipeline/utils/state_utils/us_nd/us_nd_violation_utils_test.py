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
"""Tests for the us_nd_violation_utils.py file."""
import datetime
import unittest

import attr

from recidiviz.calculator.pipeline.utils.state_utils.us_nd import us_nd_violation_utils
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationTypeEntry,
)

STATE_CODE = "US_ND"


class TestPrepareViolationResponsesForCalculations(unittest.TestCase):
    """Tests the us_nd_prepare_violation_responses_for_calculations function."""

    def test_us_nd_prepare_violation_responses_for_calculations(self) -> None:
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            state_code=STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            supervision_violation_response_id=123,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        other_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            state_code=STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        duplicate_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            supervision_violation_response_id=123,
            supervision_violation=other_supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        prepared_responses = (
            us_nd_violation_utils.us_nd_prepare_violation_responses_for_calculations(
                violation_responses=[ssvr, duplicate_ssvr]
            )
        )

        self.assertEqual([duplicate_ssvr], prepared_responses)

    def test_us_nd_prepare_violation_responses_for_calculations_multiple_types(
        self,
    ) -> None:
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            state_code=STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            supervision_violation_response_id=123,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        duplicate_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            state_code=STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        duplicate_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            supervision_violation_response_id=123,
            supervision_violation=duplicate_supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        prepared_responses = (
            us_nd_violation_utils.us_nd_prepare_violation_responses_for_calculations(
                violation_responses=[ssvr, duplicate_ssvr]
            )
        )

        expected_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            state_code=STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        expected_response = attr.evolve(ssvr, supervision_violation=expected_violation)

        self.assertEqual([expected_response], prepared_responses)

    def test_us_nd_prepare_violation_responses_for_calculations_different_days(
        self,
    ) -> None:
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            state_code=STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            supervision_violation_response_id=123,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        other_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            state_code=STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=STATE_CODE,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        other_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=STATE_CODE,
            supervision_violation_response_id=123,
            supervision_violation=other_supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        prepared_responses = (
            us_nd_violation_utils.us_nd_prepare_violation_responses_for_calculations(
                violation_responses=[ssvr, other_ssvr]
            )
        )

        self.assertCountEqual(
            [ssvr, other_ssvr],
            prepared_responses,
        )
