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
"""Incarceration metrics we calculate."""
import abc
from datetime import date
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.event_utils import (
    SupervisionLocationMixin,
    ViolationHistoryMixin,
)
from recidiviz.calculator.pipeline.utils.metric_utils import (
    AssessmentMetricMixin,
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
    SecondaryPersonExternalIdMetric,
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


class IncarcerationMetricType(RecidivizMetricType):
    """The type of incarceration metrics."""

    INCARCERATION_ADMISSION = "INCARCERATION_ADMISSION"
    INCARCERATION_COMMITMENT_FROM_SUPERVISION = (
        "INCARCERATION_COMMITMENT_FROM_SUPERVISION"
    )
    INCARCERATION_POPULATION = "INCARCERATION_POPULATION"
    INCARCERATION_RELEASE = "INCARCERATION_RELEASE"


@attr.s
class IncarcerationMetric(
    RecidivizMetric[IncarcerationMetricType],
    PersonLevelMetric,
    SecondaryPersonExternalIdMetric,
):
    """Models a single incarceration metric.

    Contains all of the identifying characteristics of the metric, including required characteristics for normalization
    as well as optional characteristics for slicing the data.
    """

    # Required characteristics
    metric_type_cls = IncarcerationMetricType

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Month
    month: int = attr.ib(default=None)

    # Optional characteristics

    # Facility
    facility: Optional[str] = attr.ib(default=None)

    # County of residence
    county_of_residence: Optional[str] = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class IncarcerationPopulationMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains incarceration population information on a given date."""

    @classmethod
    def get_description(cls) -> str:
        return "TODO(#7563): Add IncarcerationPopulationMetric description"

    # Required characteristics

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False, default=IncarcerationMetricType.INCARCERATION_POPULATION
    )

    # Date of the incarceration population count
    date_of_stay: date = attr.ib(default=None)

    # Optional characteristics

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


@attr.s
class IncarcerationAdmissionMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains admission information."""

    @classmethod
    def get_description(cls) -> str:
        return "TODO(#7563): Add IncarcerationAdmissionMetric description"

    # Required characteristics

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False, default=IncarcerationMetricType.INCARCERATION_ADMISSION
    )

    # Most relevant admission reason for a continuous stay in prison. For example, in some states, if the initial
    # incarceration period has an admission reason of TEMPORARY_CUSTODY, the admission reason is drawn from the
    # subsequent admission period, if present.
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # Admission reason raw text
    admission_reason_raw_text: Optional[str] = attr.ib(default=None)

    # TODO(#3275): Rename to purpose_for_incarceration
    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    # Admission date
    admission_date: date = attr.ib(default=None)


@attr.s
class IncarcerationCommitmentFromSupervisionMetric(
    IncarcerationAdmissionMetric,
    SupervisionLocationMixin,
    AssessmentMetricMixin,
    ViolationHistoryMixin,
):
    """Subclass of IncarcerationAdmissionMetric for admissions to incarceration that
    qualify as a commitment from supervision. Tracks information about the supervision
    that preceded the admission to incarceration."""

    @classmethod
    def get_description(cls) -> str:
        return (
            "TODO(#7563): Add IncarcerationCommitmentFromSupervisionMetric description"
        )

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False,
        default=IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
    )

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


@attr.s
class IncarcerationReleaseMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains release information."""

    @classmethod
    def get_description(cls) -> str:
        return "TODO(#7563): Add IncarcerationReleaseMetric description"

    # Required characteristics

    # The type of IncarcerationMetric
    metric_type: IncarcerationMetricType = attr.ib(
        init=False, default=IncarcerationMetricType.INCARCERATION_RELEASE
    )

    # Release date
    release_date: date = attr.ib(default=None)

    # Release reason
    release_reason: Optional[StateIncarcerationPeriodReleaseReason] = attr.ib(
        default=None
    )

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

    # Most relevant admission reason for a continuous stay in prison. For example, in some states, if the initial
    # incarceration period has an admission reason of TEMPORARY_CUSTODY, the admission reason is drawn from the
    # subsequent admission period, if present.
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # The length, in days, of the continuous stay in prison.
    total_days_incarcerated: Optional[int] = attr.ib(default=None)
