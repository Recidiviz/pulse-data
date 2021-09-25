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
"""Events related to incarceration."""
from datetime import date
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.event_utils import (
    AssessmentEventMixin,
    IdentifierEvent,
    SupervisionLocationMixin,
    ViolationHistoryMixin,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)


@attr.s(frozen=True)
class IncarcerationEvent(IdentifierEvent):
    """Models details related to an incarceration event.

    Describes a date on which a person interacted with incarceration. This includes the information pertaining to the
    interaction that we will want to track when calculating incarceration metrics.
    """

    # Whether the period corresponding to the event is counted in the state's population
    included_in_state_population: bool = attr.ib(default=True)

    # Facility
    facility: Optional[str] = attr.ib(default=None)

    # County of residence
    county_of_residence: Optional[str] = attr.ib(default=None)


@attr.s(frozen=True)
class IncarcerationStayEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person spent time incarcerated during the given day."""

    # The most serious offense NCIC code connected to the sentence group of the incarceration period from which
    # this stay event is derived
    most_serious_offense_ncic_code: Optional[str] = attr.ib(default=None)

    # The most serious offense statute connected to the sentence group of the incarceration period from which
    # this stay event is derived
    most_serious_offense_statute: Optional[str] = attr.ib(default=None)

    # The most recent "official" admission reason for this time of incarceration
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # Raw text value of the most recent "official" admission reason for this time of incarceration
    admission_reason_raw_text: Optional[str] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this incarceration
    judicial_district_code: Optional[str] = attr.ib(default=None)

    # TODO(#3275): Rename to purpose_for_incarceration
    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    @property
    def date_of_stay(self) -> date:
        return self.event_date


@attr.s(frozen=True)
class IncarcerationAdmissionEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person was admitted to incarceration for
    any reason. Used only to store shared admission attributes between subclasses, and
    cannot be instantiated as a class."""

    # Most relevant admission reason for a continuous stay in prison. For example, in
    # some states, if the initial incarceration period has an admission reason of
    # TEMPORARY_CUSTODY, the admission reason is drawn from the subsequent admission
    # period, if present.
    admission_reason: StateIncarcerationPeriodAdmissionReason = attr.ib(default=None)

    # Admission reason raw text
    admission_reason_raw_text: Optional[str] = attr.ib(default=None)

    # TODO(#3275): Rename to purpose_for_incarceration
    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    @property
    def admission_date(self) -> date:
        return self.event_date

    def __attrs_post_init__(self) -> None:
        if self.__class__ == IncarcerationAdmissionEvent:
            raise Exception(
                "Cannot instantiate IncarcerationAdmissionEvent directly; "
                "use an applicable subclass instead."
            )


@attr.s(frozen=True)
class IncarcerationCommitmentFromSupervisionAdmissionEvent(
    IncarcerationAdmissionEvent,
    AssessmentEventMixin,
    ViolationHistoryMixin,
    SupervisionLocationMixin,
):
    """Models an IncarcerationAdmissionEvent where the admission to incarceration
    is a commitment from supervision due to a sanction or revocation."""

    # A string subtype to capture more information about the
    # specialized_purpose_for_incarceration, e.g. the length of stay for a
    # SHOCK_INCARCERATION admission
    purpose_for_incarceration_subtype: Optional[str] = attr.ib(default=None)

    # Type of supervision the person was committed from
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    # The type of supervision case
    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    # Level of supervision
    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    # Raw text of the level of supervision
    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    # External ID of the officer who was supervising the person described by this
    # metric.
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # A string representation of the violations recorded in the period leading up to the
    # commitment to incarceration, which is the number of each of the represented types
    # separated by a semicolon
    violation_history_description: Optional[str] = attr.ib(default=None)

    # A list of a list of strings for each violation type and subtype recorded during
    # the period leading up to the commitment admission. The elements of the outer list
    # represent every StateSupervisionViolation that was reported in the period leading
    # up to the admission. Each inner list represents all of the violation types and
    # conditions that were listed on the given violation. For example, 3 violations may
    # be represented as: [['FELONY', 'TECHNICAL'], ['MISDEMEANOR'],
    # ['ABSCONDED', 'MUNICIPAL']]
    violation_type_frequency_counter: Optional[List[List[str]]] = attr.ib(default=None)

    # The most severe decision on the most recent response leading up to the commitment
    # admission
    most_recent_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(default=None)


@attr.s(frozen=True)
class IncarcerationStandardAdmissionEvent(IncarcerationAdmissionEvent):
    """Models an IncarcerationAdmissionEvent where the admission to incarceration does
    not qualify as a commitment from supervision."""


@attr.s(frozen=True)
class IncarcerationReleaseEvent(IncarcerationEvent):
    """Models an IncarcerationEvent where a person was released from incarceration for any reason."""

    # Most relevant admission reason for a continuous stay in prison. For example, in some states, if the initial
    # incarceration period has an admission reason of TEMPORARY_CUSTODY, the admission reason is drawn from the
    # subsequent admission period, if present.
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # Release reason
    release_reason: StateIncarcerationPeriodReleaseReason = attr.ib(default=None)

    # Release reason raw text
    release_reason_raw_text: Optional[str] = attr.ib(default=None)

    # Type of incarceration the release was from
    purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    # Supervision type at the time of release, if any.
    supervision_type_at_release: Optional[
        StateSupervisionPeriodSupervisionType
    ] = attr.ib(default=None)

    # The length, in days, of the continuous stay in prison.
    total_days_incarcerated: Optional[int] = attr.ib(default=None)

    @property
    def release_date(self) -> date:
        return self.event_date
