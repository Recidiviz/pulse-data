# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for entity_hydration_utils.py."""

import unittest
from datetime import date

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline

from recidiviz.calculator.pipeline.utils import entity_hydration_utils
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseType
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, \
    StateSupervisionViolationResponse, StateSupervisionViolation


class TestSetViolationResponseOnIncarcerationPeriod(unittest.TestCase):
    """Tests the SetViolationResponseOnIncarcerationPeriod DoFn."""
    def testSetViolationResponseOnIncarcerationPeriod(self):
        """Tests that the hydrated StateSupervisionViolationResponse is set
        on the StateIncarcerationPeriod."""
        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION
            )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2015, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            PROBATION_REVOCATION,
            release_date=date(2020, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED,
            source_supervision_violation_response=supervision_violation_response
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=55555
        )

        hydrated_supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=123,
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION,
                supervision_violation=supervision_violation
            )

        incarceration_periods_violation_responses = {
            'incarceration_periods': [incarceration_period],
            'violation_responses': [
                hydrated_supervision_violation_response
            ]}

        expected_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=1111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='TX',
                admission_date=date(2015, 5, 30),
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                PROBATION_REVOCATION,
                release_date=date(2020, 12, 4),
                release_reason=StateIncarcerationPeriodReleaseReason.
                SENTENCE_SERVED,
                source_supervision_violation_response=
                hydrated_supervision_violation_response
            )

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(12345,
                                  incarceration_periods_violation_responses)])
                  | 'Set Supervision Violation Response on '
                    'Incarceration Period' >>
                  beam.ParDo(
                      entity_hydration_utils.
                      SetViolationResponseOnIncarcerationPeriod())
                  )

        assert_that(output, equal_to([(12345, expected_incarceration_period)]))

        test_pipeline.run()
