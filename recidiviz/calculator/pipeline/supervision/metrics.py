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
"""Supervision metrics we calculate."""

from datetime import date
from typing import Optional, List

import attr

from recidiviz.calculator.pipeline.utils.event_utils import (
    SupervisionLocationMixin,
    ViolationHistoryMixin,
)
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    PersonLevelMetric,
    RecidivizMetricType,
    AssessmentMetricMixin,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)


class SupervisionMetricType(RecidivizMetricType):
    """The type of supervision metrics."""

    SUPERVISION_COMPLIANCE = "SUPERVISION_COMPLIANCE"
    SUPERVISION_POPULATION = "SUPERVISION_POPULATION"
    SUPERVISION_OUT_OF_STATE_POPULATION = "SUPERVISION_OUT_OF_STATE_POPULATION"
    SUPERVISION_REVOCATION = "SUPERVISION_REVOCATION"
    SUPERVISION_START = "SUPERVISION_START"
    SUPERVISION_SUCCESS = "SUPERVISION_SUCCESS"
    SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED = (
        "SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED"
    )
    SUPERVISION_TERMINATION = "SUPERVISION_TERMINATION"
    SUPERVISION_DOWNGRADE = "SUPERVISION_DOWNGRADE"


@attr.s
class SupervisionMetric(
    RecidivizMetric[SupervisionMetricType], SupervisionLocationMixin
):
    """Models a single supervision metric.

    Contains all of the identifying characteristics of the metric, including required characteristics for
    normalization as well as optional characteristics for slicing the data.
    """

    # Required characteristics
    metric_type_cls = SupervisionMetricType

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Month
    month: int = attr.ib(default=None)

    # Optional characteristics

    # TODO(#2891): Consider moving this out of the base class, and making the supervision type specific to each
    #   metric type
    # Supervision Type
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    # The type of supervision case
    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    # Level of supervision
    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    # Raw text of the level of supervision
    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    # External ID of the officer who was supervising the person described by this metric.
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this supervision
    judicial_district_code: Optional[str] = attr.ib(default=None)

    # The type of government entity that has responsibility for this period of supervision
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)


@attr.s
class SupervisionPopulationMetric(
    SupervisionMetric, PersonLevelMetric, ViolationHistoryMixin, AssessmentMetricMixin
):
    """Subclass of SupervisionMetric that contains supervision population information."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_POPULATION
    )

    # Date of the supervision population count
    date_of_supervision: date = attr.ib(default=None)

    # Optional characteristics

    # TODO(#3600): This field should be removed because the daily output makes this unnecessary
    # For person-level metrics only, indicates whether this person was on supervision at the end of the month
    is_on_supervision_last_day_of_month: Optional[bool] = attr.ib(default=None)

    # The projected end date for the person's supervision term.
    projected_end_date: Optional[date] = attr.ib(default=None)


@attr.s
class SupervisionOutOfStatePopulationMetric(SupervisionPopulationMetric):
    """Subclass of SupervisionPopulationMetric that contains supervision information for people who are serving their
    supervisions in another state."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION
    )


# TODO(#6988): Delete this metric once we've transitioned to the
#  IncarcerationCommitmentFromSupervisionMetric
@attr.s
class SupervisionRevocationMetric(
    SupervisionMetric, PersonLevelMetric, AssessmentMetricMixin, ViolationHistoryMixin
):
    """Subclass of SupervisionMetric that contains supervision revocation information."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_REVOCATION
    )

    # Optional characteristics

    # TODO(#6988): This will temporarily store StateSpecializedPurposeForIncarceration
    #  values as we transition to the IncarcerationCommitmentFromSupervisionMetric
    # The type of revocation of supervision
    revocation_type: Optional[StateSpecializedPurposeForIncarceration] = attr.ib(
        default=None
    )

    # A string subtype that provides further insight into the revocation_type above.
    revocation_type_subtype: Optional[str] = attr.ib(default=None)

    # For person-level metrics only, the date of the revocation admission
    revocation_admission_date: date = attr.ib(default=None)

    # A string representation of the violations recorded in the period leading up to the revocation, which is the
    # number of each of the represented types separated by a semicolon
    violation_history_description: Optional[str] = attr.ib(default=None)

    # A list of a list of strings for each violation type and subtype recorded during the period leading up to the
    # revocation. The elements of the outer list represent every StateSupervisionViolation that was reported in the
    # period leading up to the revocation. Each inner list represents all of the violation types and conditions that
    # were listed on the given violation. For example, 3 violations may be represented as:
    # [['FELONY', 'TECHNICAL'], ['MISDEMEANOR'], ['ABSCONDED', 'MUNICIPAL']]
    violation_type_frequency_counter: Optional[List[List[str]]] = attr.ib(default=None)

    # The most severe decision on the most recent response leading up to the revocation
    most_recent_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(default=None)


@attr.s
class SupervisionSuccessMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains supervision success and failure counts."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_SUCCESS
    )

    # Whether this represents a successful completion
    successful_completion: bool = attr.ib(default=None)


@attr.s
class SuccessfulSupervisionSentenceDaysServedMetric(
    SupervisionMetric, PersonLevelMetric
):
    """Subclass of SupervisionMetric that contains the average number of days served for successful supervision
    sentences with projected completion dates in the month of the metric, where the person did not spend any time
    incarcerated in the duration of the sentence."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False,
        default=SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED,
    )

    # Days served for this sentence
    days_served: int = attr.ib(default=None)


@attr.s
class SupervisionTerminationMetric(
    SupervisionMetric, PersonLevelMetric, ViolationHistoryMixin, AssessmentMetricMixin
):
    """Subclass of SupervisionMetric that contains information about a supervision that has been terminated, the reason
    for the termination, and the change in assessment score between the last assessment and the first reassessment."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_TERMINATION
    )

    # Optional characteristics

    # Change in scores between the assessment right before termination and first reliable assessment while on
    # supervision. The first "reliable" assessment is determined by state-specific logic.
    assessment_score_change: float = attr.ib(default=None)

    # The reason the supervision was terminated
    termination_reason: Optional[StateSupervisionPeriodTerminationReason] = attr.ib(
        default=None
    )

    # The date the supervision was terminated
    termination_date: Optional[date] = attr.ib(default=None)


@attr.s
class SupervisionStartMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains information about the start of supervision."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_START
    )

    # Optional characteristics

    # The reason the supervision was started
    is_official_supervision_admission: bool = attr.ib(default=False)
    admission_reason: Optional[StateSupervisionPeriodAdmissionReason] = attr.ib(
        default=None
    )

    # The date the supervision was started
    start_date: Optional[date] = attr.ib(default=None)


@attr.s
class SupervisionCaseComplianceMetric(SupervisionPopulationMetric):
    """Subclass of SupervisionPopulationMetric for people who are on supervision on a given day that records
    information regarding whether a supervision case is meeting compliance standards, as well as counts of
    compliance-related tasks that occurred in the month of the evaluation."""

    # Required characteristics

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_COMPLIANCE
    )

    # The date the on which the case's compliance was evaluated
    date_of_evaluation: date = attr.ib(default=None)

    # The number of risk assessments conducted on this person on the date_of_evaluation
    assessment_count: int = attr.ib(default=None)

    # The number of face-to-face contacts with this person on the date_of_evaluation
    face_to_face_count: int = attr.ib(default=None)

    # The number of home visits conducted on this person on the date_of_evaluation
    home_visit_count: int = attr.ib(default=None)

    # Optional characteristics

    # The date that the last assessment happened. If no assessment has yet happened, this is None.
    most_recent_assessment_date: Optional[date] = attr.ib(default=None)

    # The number of days that an assessment is overdue according to compliance standards. If it is not overdue,
    # its value is zero. We set it to None if we do not know the compliance standards for this person.
    num_days_assessment_overdue: Optional[int] = attr.ib(default=None)

    # The date that the last face-to-face contact happened. If no meetings have yet happened, this is None.
    most_recent_face_to_face_date: Optional[date] = attr.ib(default=None)

    # Whether or not the supervision officer has had face-to-face contact with the person on supervision recently
    # enough to satisfy compliance measures. Should be unset if we do not know the compliance standards for this person.
    face_to_face_frequency_sufficient: Optional[bool] = attr.ib(default=None)

    # The date that the last home visit contact happened. If no meetings have yet happened, this is None.
    most_recent_home_visit_date: Optional[date] = attr.ib(default=None)

    # Whether or not the supervision officer has conducted home visits with the person on supervision recently
    # enough to satisfy compliance measures. Should be unset if we do not know the
    # home visit compliance standards for this person.
    home_visit_frequency_sufficient: Optional[bool] = attr.ib(default=None)


@attr.s
class SupervisionDowngradeMetric(SupervisionMetric, PersonLevelMetric):
    """
    Subclass of SupervisionMetric for people whose supervision level has been downgraded.

    Note: This metric only identifies supervision level downgrades for states where a new supervision period is created
    if the supervision level changes.
    """

    # The type of SupervisionMetric
    metric_type: SupervisionMetricType = attr.ib(
        init=False, default=SupervisionMetricType.SUPERVISION_DOWNGRADE
    )

    # The date on which the downgrade in supervision level took place
    date_of_downgrade: date = attr.ib(default=None)

    # The previous supervision level, prior to the downgrade
    previous_supervision_level: StateSupervisionLevel = attr.ib(default=None)

    # The new supervision level, after the downgrade
    supervision_level: StateSupervisionLevel = attr.ib(default=None)
