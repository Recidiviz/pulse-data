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
"""Utils for hydrating connections between entities."""
from typing import Dict, Any

from more_itertools import one
import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.persistence.entity.state import entities


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(beam.typehints.Tuple[int, entities.StateIncarcerationPeriod])
class SetViolationResponseOnIncarcerationPeriod(beam.DoFn):
    """Sets a hydrated StateSupervisionviolationResponse onto the corresponding
    StateIncarcerationPeriod."""

    def process(self, element, *args, **kwargs):
        """For the incarceration periods and supervision violation responses of
        a given person, finds the matching hydrated supervision violation
        response for a resulting incarceration period, and sets the hydrated
        version onto the incarceration_period entity.

        Args:
            element: a tuple containing person_id and a dictionary of the
                person's StateIncarcerationPeriods and
                StateSupervisionviolationResponses

        Yields:
            For each incarceration period, a tuple containing the person_id and
            the incarceration_period.
        """
        person_id, incarceration_periods_violation_responses = element

        # Get the StateIncarcerationPeriods as a list
        incarceration_periods = \
            list(incarceration_periods_violation_responses[
                'incarceration_periods'])

        # Get the StateSupervisionViolationResponses as a list
        violation_responses = \
            list(incarceration_periods_violation_responses[
                'violation_responses'])

        if incarceration_periods:
            for incarceration_period in incarceration_periods:
                if incarceration_period.source_supervision_violation_response \
                        and violation_responses:

                    corresponding_response = [
                        response for response in violation_responses
                        if response.supervision_violation_response_id ==
                        incarceration_period.
                        source_supervision_violation_response.
                        supervision_violation_response_id]

                    # If there's a corresponding response, there should only
                    # be 1 (this is enforced at a DB level)
                    response = one(corresponding_response)

                    incarceration_period. \
                        source_supervision_violation_response = response

                yield (person_id, incarceration_period)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.


@with_input_types(beam.typehints.Tuple[int, Dict[str, Any]])
@with_output_types(
    beam.typehints.Tuple[int, entities.StateSupervisionViolationResponse])
class SetViolationOnViolationsResponse(beam.DoFn):
    """Sets a hydrated StateSupervisionviolation onto the corresponding
    StateSupervisionviolationResponse."""

    def process(self, element, *args, **kwargs):
        """For the supervision violations and supervision violation responses of
        a given person, finds the matching hydrated supervision violation
        for a resulting supervision violation response, and sets the hydrated
        version onto the response entity.

        Args:
            element: a tuple containing person_id and a dictionary of the
                person's StateSupervisionViolations and
                StateSupervisionViolationResponses

        Yields:
            For each response, a tuple containing the person_id and
            the response.
        """
        person_id, violations_and_responses = element

        # Get the StateSupervisionViolations as a list
        violations = \
            list(violations_and_responses[
                'violations'])

        # Get the StateSupervisionViolationResponses as a list
        violation_responses = \
            list(violations_and_responses[
                'violation_responses'])

        if violation_responses:
            for violation_response in violation_responses:
                if violations:
                    for violation in violations:
                        response_ids = [
                            response.supervision_violation_response_id for
                            response in
                            violation.supervision_violation_responses
                        ]

                        if violation_response.\
                                supervision_violation_response_id in \
                                response_ids:
                            violation_response.supervision_violation = violation

                            # Escape the inner loop when the supervision violation has been set
                            break

                yield (person_id, violation_response)

    def to_runner_api_parameter(self, _):
        pass  # Passing unused abstract method.
