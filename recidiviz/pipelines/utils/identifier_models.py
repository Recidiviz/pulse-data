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
"""Models for the events or spans that are a product of each pipeline's identifier step."""
import datetime
from typing import Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)


@attr.s
class IdentifierResult(BuildableAttr):
    """Base class for results created by the identifier step of each pipeline."""

    state_code: str = attr.ib(
        metadata={"description": "The state where the event took place"}
    )


@attr.s
class Event(IdentifierResult):
    """Base class for events created by the identifier step of each pipeline.

    Events have additional attributes that describe what was true on the given day. The
    event has an event_date, although other dates may be present.
    """

    event_date: datetime.date = attr.ib(metadata={"description": "Date of the event"})


@attr.s(frozen=True)
class Span(IdentifierResult):
    """Base class for spans created by the identifier step of each pipeline.

    Spans have additional attributes that describe what was true during the period of
    time covered by the span.
    """

    start_date_inclusive: datetime.date = attr.ib(
        metadata={
            "description": (
                "Date the span began, inclusive (attributes are valid on this day)"
            )
        }
    )

    end_date_exclusive: Optional[datetime.date] = attr.ib(
        metadata={
            "description": (
                "Date the span ended, exclusive (attributes are valid through "
                "the prior day)"
            )
        }
    )


@attr.s(frozen=True)
class ViolationHistoryMixin(BuildableAttr):
    """Set of attributes to store information about violation and response history."""

    most_severe_violation_type: Optional[StateSupervisionViolationType] = attr.ib(
        default=None,
        metadata={
            "description": (
                "The most severe violation type leading up to the date of and event"
            )
        },
    )

    most_severe_violation_type_subtype: Optional[str] = attr.ib(
        default=None,
        metadata={
            "description": (
                "A string subtype that provides further insight into the "
                "most_severe_violation_type."
            )
        },
    )

    most_severe_violation_id: Optional[int] = attr.ib(
        default=None,
        metadata={
            "description": "Id of violation attached to most_severe_violation_type"
        },
    )

    violation_history_id_array: Optional[str] = attr.ib(
        default=None,
        metadata={
            "description": (
                "Comma-separated list of violation ids considered when "
                "calculating any of these violation history fields"
            )
        },
    )

    response_count: Optional[int] = attr.ib(
        default=0,
        metadata={
            "description": (
                "The number of responses that were included in determining the "
                "most severe type/subtype"
            )
        },
    )

    most_severe_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(
        default=None,
        metadata={
            "description": (
                "The most severe decision on the responses that were included "
                "in determining the most severe type/subtype"
            )
        },
    )


@attr.s(frozen=True)
class ViolationResponseMixin(BuildableAttr):
    """Set of attributes to store information about a violation and response at a point in time."""

    violation_type: StateSupervisionViolationType = attr.ib(
        default=None, metadata={"description": "Violation type"}
    )

    violation_type_subtype: Optional[str] = attr.ib(
        default=None,
        metadata={
            "description": (
                "A string subtype that provides further insight into the "
                "violation_type above."
            )
        },
    )

    is_most_severe_violation_type: Optional[bool] = attr.ib(
        default=None,
        metadata={
            "description": (
                "Whether the violation_type recorded on this metric is the "
                "most severe out of all violation types that share the same "
                "supervision_violation_id"
            )
        },
    )

    violation_date: Optional[datetime.date] = attr.ib(
        default=None,
        metadata={
            "description": (
                "Violation date - the date the violating behavior occurred, "
                "if recorded"
            )
        },
    )

    is_violent: Optional[bool] = attr.ib(
        default=None,
        metadata={"description": "Whether the violation was violent in nature"},
    )

    is_sex_offense: Optional[bool] = attr.ib(
        default=None,
        metadata={"description": "Whether the violation was a sex offense"},
    )

    most_severe_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(
        default=None,
        metadata={
            "description": (
                "The most severe decision on the response to the associated "
                "StateSupervisionViolation"
            )
        },
    )

    is_most_severe_violation_type_of_all_violations: Optional[bool] = attr.ib(
        default=None,
        metadata={
            "description": (
                "Whether the violation type is the most severe type of all "
                "violations on a given response date"
            )
        },
    )

    is_most_severe_response_decision_of_all_violations: Optional[bool] = attr.ib(
        default=None,
        metadata={
            "description": (
                "Whether the violation response decision is the most severe "
                "of all violations on a given response date"
            )
        },
    )

    violation_type_subtype_raw_text: Optional[str] = attr.ib(
        default=None,
        metadata={
            "description": "The raw text that determines the violation_type_subtype."
        },
    )


@attr.s
class SupervisionLocationMixin(BuildableAttr):
    """Set of attributes to store supervision location information."""

    level_1_supervision_location_external_id: Optional[str] = attr.ib(
        default=None,
        metadata={
            "description": (
                "External ID of the lowest-level sub-geography (e.g. an "
                "individual office with a street address) of the officer that "
                "was supervising the person described by this object."
            )
        },
    )

    # TODO(#19343): This value is null for all states except PA and overwritten downstream
    #  by logic in BQ views. Once we update PA to hydrate supervision_site properly, we
    #  can remove this metrics field entirely.
    level_2_supervision_location_external_id: Optional[str] = attr.ib(
        default=None,
        metadata={
            "description": (
                "For states with a hierachical structure of supervision "
                "locations, this is the external ID the next-lowest-level "
                "sub-geography after level_1_supervision_sub_geography_external_id. "
                'For example, in PA this is a "district" where level 1 is '
                "an office."
            )
        },
    )


@attr.s
class InPopulationMixin:
    """Set of attributes marking whether a person was in the supervision and/or
    incarceration populations on a given date."""

    in_incarceration_population_on_date: bool = attr.ib(
        default=False,
        metadata={
            "description": (
                "Whether or not the person was counted in the incarcerated "
                "population on this date"
            )
        },
    )

    in_supervision_population_on_date: bool = attr.ib(
        default=False,
        metadata={
            "description": (
                "Whether or not the person was counted in the supervised "
                "population on this date"
            )
        },
    )


@attr.s
class IncludedInStateMixin:
    """Mixin with an attribute to marking whether a person is counted towards the state's population"""

    included_in_state_population: bool = attr.ib(
        default=True,
        metadata={
            "description": (
                "Whether the identified period is counted as part of the "
                "state's population"
            )
        },
    )


@attr.s
class AssessmentEventMixin:
    """Set of attributes that store information about assessments, and enables an event
    to be able to calculate the score bucket from assessment information."""

    assessment_type: Optional[StateAssessmentType] = attr.ib(
        default=None, metadata={"description": "Assessment type"}
    )

    assessment_score: Optional[int] = attr.ib(
        default=None,
        metadata={
            "description": "Most recent assessment score at the time of referral"
        },
    )

    assessment_level: Optional[StateAssessmentLevel] = attr.ib(
        default=None, metadata={"description": "Most recent assessment level"}
    )

    assessment_score_bucket: Optional[str] = attr.ib(
        default=None,
        metadata={
            "description": "The assessment score bucket that applies to measurement"
        },
    )
