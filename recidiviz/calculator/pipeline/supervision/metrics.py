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
from enum import Enum
from typing import Any, Dict, Optional, cast

import attr

from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric, PersonLevelMetric
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType, StateSupervisionLevel
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecision


class SupervisionMetricType(Enum):
    """The type of supervision metrics."""

    ASSESSMENT_CHANGE = 'ASSESSMENT_CHANGE'
    COMPLIANCE = 'COMPLIANCE'
    POPULATION = 'POPULATION'
    REVOCATION = 'REVOCATION'
    REVOCATION_ANALYSIS = 'REVOCATION_ANALYSIS'
    REVOCATION_VIOLATION_TYPE_ANALYSIS = 'REVOCATION_VIOLATION_TYPE_ANALYSIS'
    SUCCESS = 'SUCCESS'
    SUCCESSFUL_SENTENCE_DAYS_SERVED = 'SUCCESSFUL_SENTENCE_DAYS_SERVED'


@attr.s
class SupervisionMetric(RecidivizMetric):
    """Models a single supervision metric.

    Contains all of the identifying characteristics of the metric, including required characteristics for
    normalization as well as optional characteristics for slicing the data.
    """
    # Required characteristics

    # Year
    year: int = attr.ib(default=None)

    # Month
    month: int = attr.ib(default=None)

    # Optional characteristics

    # The number of months this metric describes, starting with the month of the metric and going back in time.
    metric_period_months: Optional[int] = attr.ib(default=None)

    # TODO(2891): Consider moving this out of the base class, and making the supervision type specific to each
    #   metric type
    # Supervision Type
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(default=None)

    # The type of supervision case
    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    # External ID of the officer who was supervising the people described by this metric.
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the district of the officer that was supervising the people described by this metric.
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this supervision
    judicial_district_code: Optional[str] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> Optional['SupervisionMetric']:
        """Builds a SupervisionMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionMetric,
                                  SupervisionMetric.build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionPopulationMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains supervision population counts."""
    # Required characteristics

    # Population count
    count: int = attr.ib(default=None)

    # Date of the supervision population count
    date_of_supervision: date = attr.ib(default=None)

    # Optional characteristics

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # The most severe violation type leading up to the revocation
    most_severe_violation_type: Optional[StateSupervisionViolationType] = attr.ib(default=None)

    # A string subtype that provides further insight into the most_severe_violation_type above.
    most_severe_violation_type_subtype: Optional[str] = attr.ib(default=None)

    # The number of violation responses leading up to the revocation
    response_count: Optional[int] = attr.ib(default=None)

    # Level of supervision
    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    # Raw text of the level of supervision
    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    # TODO(3600): This field should be removed because the daily output makes this unnecessary
    # For person-level metrics only, indicates whether this person was on supervision at the end of the month
    is_on_supervision_last_day_of_month: Optional[bool] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> Optional['SupervisionPopulationMetric']:
        """Builds a SupervisionPopulationMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionPopulationMetric,
                                  SupervisionPopulationMetric.build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionRevocationMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains supervision revocation counts."""
    # Required characteristics

    # Revocation count
    count: int = attr.ib(default=None)

    # Optional characteristics

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # The StateSupervisionViolationResponseRevocationType enum for the type of revocation of supervision that this
    # metric describes
    revocation_type: Optional[StateSupervisionViolationResponseRevocationType] = attr.ib(default=None)

    # StateSupervisionViolationType enum for the type of violation that eventually caused the revocation of supervision
    source_violation_type: Optional[StateSupervisionViolationType] = attr.ib(default=None)

    # For person-level metrics only, the date of the revocation admission
    revocation_admission_date: date = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> Optional['SupervisionRevocationMetric']:
        """Builds a SupervisionRevocationMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionRevocationMetric,
                                  SupervisionRevocationMetric.build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionRevocationAnalysisMetric(SupervisionRevocationMetric, PersonLevelMetric):
    """Subclass of SupervisionRevocationMetric that contains information for supervision revocation analysis."""

    # The most severe violation type leading up to the revocation
    most_severe_violation_type: Optional[StateSupervisionViolationType] = attr.ib(default=None)

    # A string subtype that provides further insight into the most_severe_violation_type above.
    most_severe_violation_type_subtype: Optional[str] = attr.ib(default=None)

    # The most severe decision on a response leading up to the revocation
    most_severe_response_decision: Optional[StateSupervisionViolationResponseDecision] = attr.ib(default=None)

    # The number of violation responses leading up to the revocation
    response_count: Optional[int] = attr.ib(default=None)

    # A string representation of the violations recorded in the period leading up to the revocation, which is the
    # number of each of the represented types separated by a semicolon
    violation_history_description: Optional[str] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> \
            Optional['SupervisionRevocationAnalysisMetric']:
        """Builds a SupervisionRevocationAnalysisMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionRevocationAnalysisMetric,
                                  SupervisionRevocationAnalysisMetric.build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionRevocationViolationTypeAnalysisMetric(SupervisionMetric):
    """Subclass of SupervisionRevocationMetric that contains information for
    analysis of the frequency of violation types reported leading up to revocation."""

    # The number of violations with this type recorded
    count: int = attr.ib(default=None)

    # The violation type or subtype
    violation_count_type: str = attr.ib(default=None)

    # Optional characteristics

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # The StateSupervisionViolationResponseRevocationType enum for the type of revocation of supervision that this
    # metric describes
    revocation_type: Optional[StateSupervisionViolationResponseRevocationType] = attr.ib(default=None)

    # StateSupervisionViolationType enum for the type of violation that eventually caused the revocation of supervision
    source_violation_type: Optional[StateSupervisionViolationType] = attr.ib(default=None)

    # The most severe violation type leading up to the revocation
    most_severe_violation_type: Optional[StateSupervisionViolationType] = attr.ib(default=None)

    # A string subtype that provides further insight into the most_severe_violation_type above.
    most_severe_violation_type_subtype: Optional[str] = attr.ib(default=None)

    # The number of violation responses leading up to the revocation
    response_count: Optional[int] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['SupervisionRevocationViolationTypeAnalysisMetric']:
        """Builds a SupervisionRevocationViolationTypeAnalysisMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionRevocationViolationTypeAnalysisMetric,
                                  SupervisionRevocationViolationTypeAnalysisMetric.
                                  build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionSuccessMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains supervision success and failure counts."""
    # Required characteristics

    # Number of successful completions
    successful_completion_count: int = attr.ib(default=None)

    # Total number of projected completions
    projected_completion_count: int = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> Optional['SupervisionSuccessMetric']:
        """Builds a SupervisionSuccessMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionSuccessMetric, SupervisionSuccessMetric.build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SuccessfulSupervisionSentenceDaysServedMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains the average number of days served for successful supervision
    sentences with projected completion dates in the month of the metric, where the person did not spend any time
    incarcerated in the duration of the sentence."""
    # Required characteristics

    # Number of successful completions with projected completion dates in the metric month
    successful_completion_count: int = attr.ib(default=None)

    # Average days served among the successfully completed sentences
    average_days_served: float = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> \
            Optional['SuccessfulSupervisionSentenceDaysServedMetric']:
        """Builds a SuccessfulSupervisionSentenceDaysServedMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SuccessfulSupervisionSentenceDaysServedMetric,
                                  SuccessfulSupervisionSentenceDaysServedMetric.build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class TerminatedSupervisionAssessmentScoreChangeMetric(SupervisionMetric, PersonLevelMetric):
    """Subclass of SupervisionMetric that contains counts of supervision
    that have been terminated, the reason for the termination, and the
    average change in assessment score between the last assessment and the
    first reassessment."""
    # Required characteristics

    # Number of terminated supervisions
    count: int = attr.ib(default=None)

    # Average change in scores between termination and first reassessment
    average_score_change: float = attr.ib(default=None)

    # Optional characteristics

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # The reason the supervisions were terminated
    termination_reason: Optional[StateSupervisionPeriodTerminationReason] = attr.ib(default=None)

    # The date the supervision was terminated
    termination_date: Optional[date] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> \
            Optional['TerminatedSupervisionAssessmentScoreChangeMetric']:
        """Builds a TerminatedSupervisionAssessmentScoreChangeMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(
            TerminatedSupervisionAssessmentScoreChangeMetric,
            TerminatedSupervisionAssessmentScoreChangeMetric.build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionCaseComplianceMetric(SupervisionPopulationMetric):
    """Subclass of SupervisionPopulationMetric for people who are on supervision on a given day that records
    information regarding whether a supervision case is meeting compliance standards, as well as counts of
    compliance-related tasks that occurred in the month of the evaluation."""

    # The date the on which the case's compliance was evaluated
    date_of_evaluation: date = attr.ib(default=None)

    # The number of risk assessments conducted on this person in the month of the date_of_evaluation, preceding the
    # date_of_evaluation
    assessment_count: int = attr.ib(default=None)

    # Whether or not a risk assessment has been completed for this person with enough recency to satisfy compliance
    # measures. Should be unset if we do not know the compliance standards for this person.
    assessment_up_to_date: Optional[bool] = attr.ib(default=None)

    # The number of face-to-face contacts with this person in the month of the date_of_evaluation, preceding the
    # date_of_evaluation
    face_to_face_count: int = attr.ib(default=None)

    # Whether or not the supervision officer has had face-to-face contact with the person on supervision recently
    # enough to satisfy compliance measures. Should be unset if we do not know the compliance standards for this person.
    face_to_face_frequency_sufficient: Optional[bool] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> \
            Optional['SupervisionCaseComplianceMetric']:
        """Builds a SupervisionCaseComplianceMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionCaseComplianceMetric,
                                  SupervisionCaseComplianceMetric.build_from_dictionary(metric_key))

        return supervision_metric
