# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for working with StateSupervisionViolations and their related entities in
metric pipelines."""
import sys
from collections import OrderedDict, defaultdict
from datetime import date
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.metrics.utils.calculator_utils import safe_list_index
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    StateSupervisionViolationResponseT,
    get_most_severe_response_decision,
    violation_responses_in_window,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

SUBSTANCE_ABUSE_SUBTYPE_STR: str = "SUBSTANCE_ABUSE"

# The number of months for the window of time in which violations and violation
# responses should be considered when producing metrics related to a person's violation
# history
VIOLATION_HISTORY_WINDOW_MONTHS = 12


def _shorthand_description_for_ranked_violation_counts(
    subtype_counts: Dict[str, int],
    violation_delegate: StateSpecificViolationDelegate,
) -> Optional[str]:
    """Converts the dictionary mapping types of violations to the number of that type
    into a string listing the types and counts, ordered by the violation type
    severity defined by state-specific logic. If there aren't any counts of any
    violations, returns None."""
    sorted_subtypes = sorted_violation_subtypes_by_severity(
        list(subtype_counts.keys()), violation_delegate
    )

    if not sorted_subtypes:
        return None

    ranked_shorthand_counts: Dict[str, int] = OrderedDict()
    for subtype in sorted_subtypes:
        violation_count = subtype_counts[subtype]
        if violation_count:
            # Convert to string shorthand
            ranked_shorthand_counts[
                _shorthand_for_violation_subtype(violation_delegate, subtype)
            ] = violation_count

    descriptions = [
        f"{count}{label}"
        for label, count in ranked_shorthand_counts.items()
        if count > 0
    ]

    if descriptions:
        return ";".join(descriptions)

    return None


def _identify_most_severe_violation_type_and_subtype(
    violations: List[NormalizedStateSupervisionViolation],
    violation_delegate: StateSpecificViolationDelegate,
) -> Tuple[Optional[StateSupervisionViolationType], Optional[str]]:
    """Identifies the most severe violation type on the provided |violations|, and,
    if relevant, the subtype of that most severe violation type. Returns both as a
    tuple.
    """
    violation_subtypes: List[str] = []

    if not violations:
        return None, None

    for violation in violations:
        violation_subtypes.extend(
            violation_delegate.get_violation_type_subtype_strings_for_violation(
                violation
            )
        )

    if not violation_subtypes:
        return None, None

    most_severe_subtype = most_severe_violation_subtype(
        violation_subtypes, violation_delegate
    )

    most_severe_type = None

    if most_severe_subtype:
        most_severe_type = violation_type_from_subtype(
            violation_delegate, most_severe_subtype
        )

    return most_severe_type, most_severe_subtype


def most_severe_violation_subtype(
    violation_subtypes: List[str], violation_delegate: StateSpecificViolationDelegate
) -> Optional[str]:
    """Given the |violation_delegate| and list of |violation_subtypes|, determines
    the most severe subtype present. Defers to the severity in the
    |default_severity_order| if no state-specific logic is implemented."""
    if not violation_subtypes:
        return None

    sorted_subtypes = sorted_violation_subtypes_by_severity(
        violation_subtypes, violation_delegate
    )

    if sorted_subtypes:
        return sorted_subtypes[0]

    return None


def _get_violation_type_frequency_counter(
    violations: List[NormalizedStateSupervisionViolation],
    violation_delegate: StateSpecificViolationDelegate,
) -> Optional[List[List[str]]]:
    """For every violation in violations, builds a list of strings, where each string
    is a violation type or a condition violated that is recorded on the given
    violation. Returns a list of all lists of strings, where the length of the list
    is the number of violations."""

    violation_type_frequency_counter: List[List[str]] = []

    if not violations:
        return None

    for violation in violations:
        violation_types = (
            violation_delegate.get_violation_type_subtype_strings_for_violation(
                violation
            )
        )

        if violation_types:
            violation_type_frequency_counter.append(violation_types)

    return (
        violation_type_frequency_counter if violation_type_frequency_counter else None
    )


ViolationHistory = NamedTuple(
    "ViolationHistory",
    [
        ("most_severe_violation_type", Optional[StateSupervisionViolationType]),
        ("most_severe_violation_type_subtype", Optional[str]),
        (
            "most_severe_response_decision",
            Optional[StateSupervisionViolationResponseDecision],
        ),
        ("response_count", Optional[int]),
        ("violation_history_description", Optional[str]),
        ("violation_type_frequency_counter", Optional[List[List[str]]]),
    ],
)


def get_violation_and_response_history(
    upper_bound_exclusive_date: date,
    violation_responses_for_history: List[NormalizedStateSupervisionViolationResponse],
    violation_delegate: StateSpecificViolationDelegate,
    incarceration_period: Optional[StateIncarcerationPeriod] = None,
    lower_bound_inclusive_date_override: Optional[date] = None,
) -> ViolationHistory:
    """Identifies and returns various details of the violation history on the responses
    that were recorded during a period of time.

    If a lower_bound_inclusive_date_override is provided, uses the period of time
    between the lower_bound_inclusive_date_override and the upper_bound_exclusive_date.

    If lower_bound_inclusive_date_override is null, uses the period of time
    VIOLATION_HISTORY_WINDOW_MONTHS preceding the |end_date|.
    """
    if not violation_responses_for_history and incarceration_period is None:
        return ViolationHistory(
            most_severe_violation_type=None,
            most_severe_violation_type_subtype=None,
            most_severe_response_decision=None,
            response_count=0,
            violation_history_description=None,
            violation_type_frequency_counter=None,
        )

    lower_bound_inclusive_date = (
        lower_bound_inclusive_date_override
        or upper_bound_exclusive_date
        - relativedelta(months=VIOLATION_HISTORY_WINDOW_MONTHS)
    )

    responses_in_window = violation_responses_in_window(
        violation_responses=violation_responses_for_history,
        upper_bound_exclusive=upper_bound_exclusive_date,
        lower_bound_inclusive=lower_bound_inclusive_date,
    )

    violations_in_window: List[NormalizedStateSupervisionViolation] = []
    violation_ids_in_window: Set[int] = set()

    for response in responses_in_window:
        violation = response.supervision_violation

        if not violation or not isinstance(
            violation, NormalizedStateSupervisionViolation
        ):
            raise TypeError(
                "Expected supervision_violation on "
                "NormalizedStateSupervisionViolationResponse to be of type "
                "NormalizedStateSupervisionViolation, found: "
                f"{type(violation)}."
            )
        if (
            violation.supervision_violation_id
            and violation.supervision_violation_id not in violation_ids_in_window
        ):
            violations_in_window.append(violation)
            violation_ids_in_window.add(violation.supervision_violation_id)

    # Find the most severe violation type info of all of the entries in the window
    (
        most_severe_violation_type,
        most_severe_violation_type_subtype,
    ) = _identify_most_severe_violation_type_and_subtype(
        violations_in_window, violation_delegate
    )

    violation_type_entries = []
    for violation in violations_in_window:
        violation_type_entries.extend(violation.supervision_violation_types)

    violation_history_description = _get_violation_history_description(
        violations_in_window, violation_delegate
    )

    violation_type_frequency_counter = _get_violation_type_frequency_counter(
        violations_in_window, violation_delegate
    )

    # Count the number of responses in the window
    response_count = len(responses_in_window)

    most_severe_response_decision = get_most_severe_response_decision(
        responses_in_window
    )

    violation_history_result = ViolationHistory(
        most_severe_violation_type,
        most_severe_violation_type_subtype,
        most_severe_response_decision,
        response_count,
        violation_history_description,
        violation_type_frequency_counter,
    )

    return violation_history_result


def _get_violation_history_description(
    violations: List[NormalizedStateSupervisionViolation],
    violation_delegate: StateSpecificViolationDelegate,
) -> Optional[str]:
    """Returns a string description of the violation history given the violation type
    entries. Tallies the number of each violation type, and then builds a string that
    lists the number of each of the represented types in the order listed in the
    violation_type_shorthand dictionary and separated by a semicolon.

    For example, if someone has 3 felonies and 2 technicals, this will return
    '3fel;2tech'.
    """
    if not violations:
        return None

    subtype_counts: Dict[str, int] = defaultdict(int)

    # Count all violation types and subtypes
    for violation in violations:
        most_severe_violation_type_and_subtype = (
            _identify_most_severe_violation_type_and_subtype(
                [violation], violation_delegate
            )
        )
        if not most_severe_violation_type_and_subtype:
            continue
        _, most_severe_subtype = most_severe_violation_type_and_subtype

        if most_severe_subtype:
            subtype_counts[most_severe_subtype] += 1

    return _shorthand_description_for_ranked_violation_counts(
        subtype_counts, violation_delegate
    )


def filter_violation_responses_for_violation_history(
    violation_delegate: StateSpecificViolationDelegate,
    violation_responses: List[StateSupervisionViolationResponseT],
    include_follow_up_responses: bool = False,
) -> List[StateSupervisionViolationResponseT]:
    """Returns the list of violation responses that should be included in analyses of
    violation history. Uses the state-specific code to determine whether each
    response should also be included."""
    filtered_responses = [
        response
        for response in violation_responses
        if violation_delegate.should_include_response_in_violation_history(
            response, include_follow_up_responses
        )
    ]
    return filtered_responses


def sorted_violation_subtypes_by_severity(
    violation_subtypes: List[str], violation_delegate: StateSpecificViolationDelegate
) -> List[str]:
    """Sorts the provided |violation_subtypes| by severity, and returns the list in
    order of descending severity. Follows the severity ordering returned by the
    state-specific violation delegate"""

    sorted_violation_subtypes = sorted(
        violation_subtypes,
        key=lambda subtype: safe_list_index(
            _get_violation_subtype_sort_order(violation_delegate), subtype, sys.maxsize
        ),
    )

    return sorted_violation_subtypes


def violation_type_subtypes_with_violation_type_mappings(
    violation_delegate: StateSpecificViolationDelegate,
) -> Set[str]:
    """Returns the set of violation_type_subtype values that have a defined mapping
    to a violation_type value based on the
    violation_type_and_subtype_shorthand_ordered_map of the given |violation_delegate|.
    """

    return {
        subtype
        for _, subtype, _ in violation_delegate.violation_type_and_subtype_shorthand_ordered_map
    }


def _get_violation_subtype_sort_order(
    violation_delegate: StateSpecificViolationDelegate,
) -> List[str]:
    """Returns the sort order of violation subtypes by severity. Severity order
    defined by violation_type_and_subtype_shorthand_ordered_map on the
    |violation_delegate|."""
    return [
        subtype
        for _, subtype, _ in violation_delegate.violation_type_and_subtype_shorthand_ordered_map
    ]


def violation_type_from_subtype(
    violation_delegate: StateSpecificViolationDelegate, violation_subtype: str
) -> StateSupervisionViolationType:
    """Determines which StateSupervisionViolationType corresponds to the
    |violation_subtype| value defined by the
    violation_type_and_subtype_shorthand_ordered_map on the |violation_delegate|."""

    for (
        violation_type,
        violation_subtype_value,
        _,
    ) in violation_delegate.violation_type_and_subtype_shorthand_ordered_map:
        if violation_subtype == violation_subtype_value:
            return violation_type

    raise ValueError(f"Unexpected violation_subtype {violation_subtype}.")


def _shorthand_for_violation_subtype(
    violation_delegate: StateSpecificViolationDelegate, violation_subtype: str
) -> str:
    """Returns the shorthand string representing the given |violation_subtype| in the
    given |state_code| defined by the
    violation_type_and_subtype_shorthand_ordered_map on the |violation_delegate|."""
    for (
        _,
        violation_subtype_value,
        subtype_shorthand,
    ) in violation_delegate.violation_type_and_subtype_shorthand_ordered_map:
        if violation_subtype == violation_subtype_value:
            return subtype_shorthand

    raise ValueError(f"Unexpected violation_subtype {violation_subtype}.")
