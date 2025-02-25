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
"""Contains the StateSpecificViolationDelegate, the interface
for state-specific decisions involved in categorizing various attributes of
violations."""
import abc
import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.pipelines.utils.violation_response_utils import (
    violation_responses_in_window,
)

DEFAULT_VIOLATION_TYPE_SEVERITY_ORDER: List[StateSupervisionViolationType] = [
    StateSupervisionViolationType.FELONY,
    StateSupervisionViolationType.MISDEMEANOR,
    StateSupervisionViolationType.LAW,
    StateSupervisionViolationType.ABSCONDED,
    StateSupervisionViolationType.MUNICIPAL,
    StateSupervisionViolationType.ESCAPED,
    StateSupervisionViolationType.TECHNICAL,
    StateSupervisionViolationType.INTERNAL_UNKNOWN,
    StateSupervisionViolationType.EXTERNAL_UNKNOWN,
]


class StateSpecificViolationDelegate(abc.ABC, StateSpecificDelegate):
    """Interface for state-specific decisions involved in categorizing various
    attributes of violations."""

    # Default values for the severity order of violation subtypes, with defined
    # shorthand values. Variable should be overridden by state-specific implementations
    # if the state has unique subtypes.
    violation_type_and_subtype_shorthand_ordered_map: List[
        Tuple[StateSupervisionViolationType, str, str]
    ] = [
        (violation_type, violation_type.value, violation_type.value.lower())
        for violation_type in DEFAULT_VIOLATION_TYPE_SEVERITY_ORDER
    ]

    def should_include_response_in_violation_history(
        self,
        response: NormalizedStateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,  # pylint: disable=unused-argument
    ) -> bool:
        """Determines whether the given |response| should be included in violation history analyses.

        Default behavior is to include the response if it is of type VIOLATION_REPORT or CITATION.
        Should be overridden by state-specific implementations if necessary."""
        return response.response_type in (
            StateSupervisionViolationResponseType.VIOLATION_REPORT,
            StateSupervisionViolationResponseType.CITATION,
        )

    def violation_history_window_relevant_to_critical_date(
        self,
        critical_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
    ) -> DateRange:
        """Returns the window of time before a particular critical date in which we
        should consider violations for the violation history prior to that date.

        Default behavior is to use the date of the last violation response recorded
        prior to the |critical_date| as the upper bound of the window, with a lower
        bound that is |default_violation_history_window_months| before that date.

        Should be overridden by state-specific implementations if necessary.
        """
        # We will use the date of the last response prior to the critical date as the
        # window cutoff.
        responses_before_critical_date = violation_responses_in_window(
            violation_responses=sorted_and_filtered_violation_responses,
            upper_bound_exclusive=critical_date + relativedelta(days=1),
            lower_bound_inclusive=None,
        )

        violation_history_end_date = critical_date

        if responses_before_critical_date:
            # If there were violation responses leading up to the critical
            # date, then we want the violation history leading up to the last
            # response_date instead of the critical_date
            last_response = responses_before_critical_date[-1]

            if not last_response.response_date:
                # This should never happen, but is here to silence mypy warnings
                # about empty response_dates.
                raise ValueError(
                    "Not effectively filtering out responses without valid"
                    " response_dates."
                )
            violation_history_end_date = last_response.response_date

        violation_window_lower_bound_inclusive = (
            violation_history_end_date
            - relativedelta(months=default_violation_history_window_months)
        )
        violation_window_upper_bound_exclusive = (
            violation_history_end_date + relativedelta(days=1)
        )

        return DateRange(
            lower_bound_inclusive_date=violation_window_lower_bound_inclusive,
            upper_bound_exclusive_date=violation_window_upper_bound_exclusive,
        )

    def get_violation_type_subtype_strings_for_violation(
        self,
        violation: NormalizedStateSupervisionViolation,
    ) -> Dict[str, List[Optional[str]]]:
        """Returns a list of tuples that represent the violation subtypes present on
        the given |violation|, along with the raw text used to determine the subtype.

        Default behavior is to return a list of the violation_type raw values in the
        violation's supervision_violation_types.

        Should be overridden by state-specific implementations if necessary."""

        supervision_violation_types = violation.supervision_violation_types

        violation_subtypes_map: Dict[str, List[Optional[str]]] = defaultdict(list)
        for violation_type_entry in supervision_violation_types:
            if violation_type_entry.violation_type:
                violation_subtypes_map[
                    violation_type_entry.violation_type.value
                ].append(violation_type_entry.violation_type_raw_text)

        return violation_subtypes_map

    def include_decisions_on_follow_up_responses_for_most_severe_response(self) -> bool:
        """Some StateSupervisionViolationResponses are a 'follow-up' type of response, which is a state-defined response
        that is related to a previously submitted response. This returns whether or not the decision entries on
        follow-up responses should be considered in the calculation of the most severe response decision. By default,
        follow-up responses should not be considered"""
        return False
