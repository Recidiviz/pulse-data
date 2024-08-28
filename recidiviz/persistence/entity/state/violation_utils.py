# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helper functions for using objects in the StateSupervisionViolation subtree of the
StatePerson entity graph.
"""
from typing import Type, TypeVar

from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.utils.types import assert_type

StateSupervisionViolationResponseT = TypeVar(
    "StateSupervisionViolationResponseT",
    StateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponse,
)


def collect_violation_responses(
    violations: list[StateSupervisionViolation]
    | list[NormalizedStateSupervisionViolation],
    expected_response_type: Type[StateSupervisionViolationResponseT],
) -> list[StateSupervisionViolationResponseT]:
    """Given a collection of supervision violation objects, returns the unique set of
    violation responses that are referenced by those violations.

    Throws if a violation response object is referenced by multiple different
    violations.
    """
    seen_violation_response_obj_ids = set()
    unique_violation_responses = []
    for v in violations:
        for vr in v.supervision_violation_responses:
            response = assert_type(vr, expected_response_type)
            if id(response) in seen_violation_response_obj_ids:
                raise ValueError(
                    f"Found violation response that is referenced by multiple "
                    f"different violations. "
                    f"\nViolation response external id: [{vr.external_id}]"
                    f"\nViolation external id: [{v.external_id}]"
                )
            unique_violation_responses.append(response)
            seen_violation_response_obj_ids.add(id(response))
    return unique_violation_responses


def collect_unique_violations(
    violation_responses: list[NormalizedStateSupervisionViolationResponse],
) -> list[NormalizedStateSupervisionViolation]:
    """Given a collection of violation responses, returns a list of unique violation
    objects that those responses reference.
    """
    seen_violation_ids = set()
    unique_violations = []
    for vr in violation_responses:
        violation = assert_type(
            vr.supervision_violation, NormalizedStateSupervisionViolation
        )
        if id(violation) not in seen_violation_ids:
            unique_violations.append(violation)
            seen_violation_ids.add(id(violation))
    return unique_violations
