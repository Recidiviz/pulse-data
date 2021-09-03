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
"""Utils for events that are a product of each pipeline's identifier step."""
import datetime
import logging
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
class IdentifierEvent(BuildableAttr):
    """Base class for events created by the identifier step of each pipeline. The event should have an event_date,
    although other dates may be present."""

    # The state where the event took place
    state_code: str = attr.ib()

    # Date of the event
    event_date: datetime.date = attr.ib()


@attr.s(frozen=True)
class ViolationHistoryMixin(BuildableAttr):
    """Set of attributes to store information about violation and response history."""

    # The most severe violation type leading up to the date of and event
    most_severe_violation_type: Optional[StateSupervisionViolationType] = attr.ib(
        default=None
    )

    # A string subtype that provides further insight into the
    # most_severe_violation_type above.
    most_severe_violation_type_subtype: Optional[str] = attr.ib(default=None)

    # The number of responses that were included in determining the most severe
    # type/subtype
    response_count: Optional[int] = attr.ib(default=0)

    # The most severe decision on the responses that were included in determining the
    # most severe type/subtype
    most_severe_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(default=None)


@attr.s(frozen=True)
class ViolationResponseMixin(BuildableAttr):
    """Set of attributes to store information about a violation and response at a point in time."""

    # Violation type
    violation_type: StateSupervisionViolationType = attr.ib(default=None)

    # A string subtype that provides further insight into the violation_type above.
    violation_type_subtype: Optional[str] = attr.ib(default=None)

    # Whether the violation_type recorded on this metric is the most severe out of all violation types that share the same supervision_violation_id
    is_most_severe_violation_type: Optional[bool] = attr.ib(default=None)

    # Violation date - the date the violating behavior occurred, if recorded
    violation_date: Optional[datetime.date] = attr.ib(default=None)

    # Whether the violation was violent in nature
    is_violent: Optional[bool] = attr.ib(default=None)

    # Whether the violation was a sex offense
    is_sex_offense: Optional[bool] = attr.ib(default=None)

    # The most severe decision on the response to the associated StateSupervisionViolation
    most_severe_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(default=None)

    # Whether the violation type is the most severe type of all violations on a given response date
    is_most_severe_violation_type_of_all_violations: Optional[bool] = attr.ib(
        default=None
    )

    # Whether the violation response decision is the most severe of all violations on a given response date
    is_most_severe_response_decision_of_all_violations: Optional[bool] = attr.ib(
        default=None
    )


@attr.s
class SupervisionLocationMixin(BuildableAttr):
    """Set of attributes to store supervision location information."""

    # External ID of the district of the officer that was supervising the person
    # described by this object
    # TODO(#4709): THIS FIELD IS DEPRECATED - USE level_1_supervision_location_external_id and
    #  level_2_supervision_location_external_id instead.
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the lowest-level sub-geography (e.g. an individual office with a
    # street address) of the officer that was supervising the person described by this
    # object.
    level_1_supervision_location_external_id: Optional[str] = attr.ib(default=None)

    # For states with a hierachical structure of supervision locations, this is the
    # external ID the next-lowest-level sub-geography after
    # level_1_supervision_sub_geography_external_id. For example, in PA this is a
    # "district" where level 1 is an office.
    level_2_supervision_location_external_id: Optional[str] = attr.ib(default=None)


@attr.s
class InPopulationMixin:
    """Set of attributes marking whether a person was in the supervision and/or
    incarceration populations on a given date."""

    # Whether or not the person was counted in the incarcerated population on this date
    in_incarceration_population_on_date: bool = attr.ib(default=False)

    # Whether or not the person was counted in the supervised population on this date
    in_supervision_population_on_date: bool = attr.ib(default=False)


@attr.s
class AssessmentEventMixin:
    """Set of attributes that store information about assessments, and enables an event
    to be able to calculate the score bucket from assessment information."""

    DEFAULT_ASSESSMENT_SCORE_BUCKET = "NOT_ASSESSED"

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # Most recent assessment score at the time of referral
    assessment_score: Optional[int] = attr.ib(default=None)

    # Most recent assessment level
    assessment_level: Optional[StateAssessmentLevel] = attr.ib(default=None)

    @property
    def assessment_score_bucket(self) -> str:
        """Calculates the assessment score bucket that applies to measurement.

        NOTE: Only LSIR and ORAS buckets are currently supported
        TODO(#2742): Add calculation support for all supported StateAssessmentTypes

        Returns:
            A string representation of the assessment score for the person.
            DEFAULT_ASSESSMENT_SCORE_BUCKET if the assessment type is not supported or if the object is missing
                assessment information.
        """
        state_code = getattr(self, "state_code")

        if self.assessment_type:
            if self.assessment_type == StateAssessmentType.LSIR:
                if state_code == "US_PA":
                    # The score buckets for US_PA have changed over time, so we defer to the assessment_level
                    if self.assessment_level:
                        return self.assessment_level.value
                else:
                    if self.assessment_score:
                        if self.assessment_score < 24:
                            return "0-23"
                        if self.assessment_score <= 29:
                            return "24-29"
                        if self.assessment_score <= 38:
                            return "30-38"
                        return "39+"

            elif self.assessment_type in [
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT,
                StateAssessmentType.ORAS_MISDEMEANOR_SCREENING,
                StateAssessmentType.ORAS_PRE_TRIAL,
                StateAssessmentType.ORAS_PRISON_SCREENING,
                StateAssessmentType.ORAS_PRISON_INTAKE,
                StateAssessmentType.ORAS_REENTRY,
                StateAssessmentType.ORAS_STATIC,
                StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY,
            ]:
                if self.assessment_level:
                    return self.assessment_level.value
            elif self.assessment_type in [
                StateAssessmentType.INTERNAL_UNKNOWN,
                StateAssessmentType.ASI,
                StateAssessmentType.CSSM,
                StateAssessmentType.HIQ,
                StateAssessmentType.PA_RST,
                StateAssessmentType.PSA,
                StateAssessmentType.SORAC,
                StateAssessmentType.STATIC_99,
                StateAssessmentType.TCU_DRUG_SCREEN,
            ]:
                logging.warning(
                    "Assessment type %s is unsupported.", self.assessment_type
                )
            else:
                raise ValueError(
                    f"Unexpected unsupported StateAssessmentType: {self.assessment_type}"
                )

        return self.DEFAULT_ASSESSMENT_SCORE_BUCKET
