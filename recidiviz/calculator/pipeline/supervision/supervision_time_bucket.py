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
"""Buckets of time on supervision that may have included a revocation."""
from datetime import date
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.event_utils import (
    AssessmentEventMixin,
    IdentifierEventWithSingularDate,
    ViolationHistoryMixin,
    SupervisionLocationMixin,
)
from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import BuildableAttr
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
    is_official_supervision_admission,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)


# TODO(#5307): Convert all "bucket" language to use "event"
@attr.s(frozen=True)
class SupervisionTimeBucket(
    IdentifierEventWithSingularDate, SupervisionLocationMixin, AssessmentEventMixin
):
    """Models details related to a bucket of time on supervision.

    Describes a month in which a person spent any amount of time on supervision. This includes the information
    pertaining to time on supervision that we will want to track when calculating supervision and revocation metrics."""

    # Year for when the person was on supervision
    year: int = attr.ib(default=None, validator=attr_validators.is_int)

    # Month for when the person was on supervision
    month: int = attr.ib(default=None, validator=attr_validators.is_int)

    # TODO(#2891): Consider moving this out of the base class, and making the supervision type specific to each
    #   bucket type
    # The type of supervision the person was on on the last day of the time bucket
    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    # Level of supervision
    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    # Raw text of the level of supervision
    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    # The type of supervision case
    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    # External ID of the officer who was supervising the people described by this metric
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this supervision
    judicial_district_code: Optional[str] = attr.ib(default=None)

    # The type of government entity that has responsibility for this period of supervision
    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)

    @property
    def date_of_evaluation(self) -> date:
        return self.event_date


@attr.s(frozen=True)
class SupervisionDowngradeBucket(BuildableAttr):
    """
    Base class for including whether a supervision level downgrade took place on a SupervisionTimeBucket.

    Note: This bucket only identifies supervision level downgrades for states where a new supervision period is created
    if the supervision level changes.
    """

    # Whether a supervision level downgrade has taken place.
    supervision_level_downgrade_occurred: bool = attr.ib(default=False)

    # The supervision level of the previous supervision period.
    previous_supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)


# TODO(#6988): Delete this bucket after we transition to the
#  IncarcerationCommitmentFromSupervisionMetric
@attr.s(frozen=True)
class RevocationReturnSupervisionTimeBucket(
    SupervisionTimeBucket, ViolationHistoryMixin
):
    """Models a SupervisionTimeBucket where the person was incarcerated for a revocation."""

    # TODO(#6988): This will temporarily store StateSpecializedPurposeForIncarceration
    #  values as we transition to the IncarcerationCommitmentFromSupervisionMetric
    # The type of revocation of supervision
    revocation_type: Optional[StateSpecializedPurposeForIncarceration] = attr.ib(
        default=None
    )

    # A string subtype that provides further insight into the revocation_type above.
    revocation_type_subtype: Optional[str] = attr.ib(default=None)

    # A string representation of the violations recorded in the period leading up to the revocation
    violation_history_description: Optional[str] = attr.ib(default=None)

    # A list of a list of strings for each violation type and subtype recorded during the period leading up to the
    # revocation. The elements of the outer list represent every StateSupervisionViolation that was reported in the
    # period leading up to the revocation. Each inner list represents all of the violation types and conditions that
    # were listed on the given violation.
    violation_type_frequency_counter: Optional[List[List[str]]] = attr.ib(default=None)

    # The most severe decision on the most recent response leading up to the revocation
    most_recent_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = attr.ib(default=None)

    # TODO(#3600): This field should be removed because the daily output makes this unnecessary
    # True if the stint of time on supervision this month included the last day of the month
    is_on_supervision_last_day_of_month: bool = attr.ib()

    # The projected end date for the person's supervision term.
    projected_end_date: Optional[date] = attr.ib(default=None)

    @is_on_supervision_last_day_of_month.default
    def _default_is_on_supervision_last_day_of_month(self) -> None:
        raise ValueError("Must set is_on_supervision_last_day_of_month!")

    @property
    def revocation_admission_date(self) -> date:
        return self.event_date

    @property
    def date_of_supervision(self) -> date:
        return self.event_date


@attr.s(frozen=True)
class NonRevocationReturnSupervisionTimeBucket(
    SupervisionTimeBucket,
    ViolationHistoryMixin,
    SupervisionDowngradeBucket,
):
    """Models a SupervisionTimeBucket where the person was not incarcerated for a revocation."""

    # TODO(#3600): This field should be removed because the daily output makes this unnecessary
    # True if the stint of time on supervision this month included the last day of the month
    is_on_supervision_last_day_of_month: bool = attr.ib()

    # The projected end date for the person's supervision term.
    projected_end_date: Optional[date] = attr.ib(default=None)

    # Information related to whether the supervision case is meeting compliance standards
    case_compliance: Optional[SupervisionCaseCompliance] = attr.ib(default=None)

    @is_on_supervision_last_day_of_month.default
    def _default_is_on_supervision_last_day_of_month(self) -> None:
        raise ValueError("Must set is_on_supervision_last_day_of_month!")

    @property
    def date_of_supervision(self) -> date:
        return self.event_date

    @property
    def date_of_downgrade(self) -> date:
        return self.event_date

    @property
    def assessment_count(self) -> Optional[int]:
        if not self.case_compliance:
            return None
        return self.case_compliance.assessment_count

    @property
    def most_recent_assessment_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.most_recent_assessment_date

    @property
    def num_days_assessment_overdue(self) -> Optional[int]:
        if not self.case_compliance:
            return None
        return self.case_compliance.num_days_assessment_overdue

    @property
    def face_to_face_count(self) -> Optional[int]:
        if not self.case_compliance:
            return None
        return self.case_compliance.face_to_face_count

    @property
    def most_recent_face_to_face_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.most_recent_face_to_face_date

    @property
    def face_to_face_frequency_sufficient(self) -> Optional[bool]:
        if not self.case_compliance:
            return None
        return self.case_compliance.face_to_face_frequency_sufficient

    @property
    def most_recent_home_visit_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.most_recent_home_visit_date

    @property
    def home_visit_count(self) -> Optional[int]:
        if not self.case_compliance:
            return None
        return self.case_compliance.home_visit_count

    @property
    def home_visit_frequency_sufficient(self) -> Optional[bool]:
        if not self.case_compliance:
            return None
        return self.case_compliance.home_visit_frequency_sufficient


@attr.s(frozen=True)
class ProjectedSupervisionCompletionBucket(SupervisionTimeBucket):
    """Models a month in which supervision was projected to complete.

    Describes whether or not the supervision was successfully completed or not, as well as other details about the time
    on supervision.
    """

    # Whether or not the supervision was completed successfully
    successful_completion: bool = attr.ib(default=True)

    # Whether or not there were any incarceration admissions during the sentence
    incarcerated_during_sentence: bool = attr.ib(default=True)

    # Length of time served on the supervision sentence, in days
    sentence_days_served: int = attr.ib(default=None)


@attr.s(frozen=True)
class SupervisionStartBucket(SupervisionTimeBucket):
    """Models details regarding the start of supervision."""

    # The reason for supervision admission
    admission_reason: Optional[StateSupervisionPeriodAdmissionReason] = attr.ib(
        default=None
    )

    @property
    def start_date(self) -> date:
        return self.event_date

    @property
    def is_official_supervision_admission(self) -> bool:
        return is_official_supervision_admission(self.admission_reason)


@attr.s(frozen=True)
class SupervisionTerminationBucket(SupervisionTimeBucket, ViolationHistoryMixin):
    """Models a month in which supervision was terminated.

    Describes the reason for termination, and the change in assessment score between
    first reassessment and termination of supervision.
    """

    # The reason for supervision termination
    termination_reason: Optional[StateSupervisionPeriodTerminationReason] = attr.ib(
        default=None
    )

    # Change in scores between the assessment right before termination and first reliable assessment while on
    # supervision. The first "reliable" assessment is determined by state-specific logic.
    assessment_score_change: Optional[int] = attr.ib(default=None)

    @property
    def termination_date(self) -> date:
        return self.event_date
