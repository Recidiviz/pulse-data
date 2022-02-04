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
"""Various events related to being on supervision."""
from datetime import date
from typing import Optional

import attr

from recidiviz.calculator.pipeline.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.event_utils import (
    AssessmentEventMixin,
    IdentifierEvent,
    InPopulationMixin,
    SupervisionLocationMixin,
    ViolationHistoryMixin,
)
from recidiviz.common import attr_validators
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
    is_official_supervision_admission,
)


@attr.s(frozen=True)
class SupervisionEvent(IdentifierEvent, SupervisionLocationMixin, AssessmentEventMixin):
    """Models details of an event related to being on supervision.

    This includes the information pertaining to time on supervision that we
    will want to track when calculating various supervision metrics."""

    # Year for when the person was on supervision
    year: int = attr.ib(default=None, validator=attr_validators.is_int)

    # Month for when the person was on supervision
    month: int = attr.ib(default=None, validator=attr_validators.is_int)

    # The supervision type
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
class SupervisionDowngradeEvent(BuildableAttr):
    """
    Base class for including whether a supervision level downgrade took place on a
    SupervisionEvent.

    Note: This event only identifies supervision level downgrades for states where a
    new supervision period is created if the supervision level changes.
    """

    # Whether a supervision level downgrade has taken place.
    supervision_level_downgrade_occurred: bool = attr.ib(default=False)

    # The supervision level of the previous supervision period.
    previous_supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)


@attr.s(frozen=True)
class SupervisionPopulationEvent(
    SupervisionEvent,
    ViolationHistoryMixin,
    SupervisionDowngradeEvent,
):
    """Models a day on which a person was on supervision."""

    # The projected end date for the person's supervision term.
    projected_end_date: Optional[date] = attr.ib(default=None)

    # Information related to whether the supervision case is meeting compliance standards
    case_compliance: Optional[SupervisionCaseCompliance] = attr.ib(default=None)

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
    def next_recommended_assessment_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.next_recommended_assessment_date

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
    def next_recommended_face_to_face_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.next_recommended_face_to_face_date

    @property
    def most_recent_home_visit_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.most_recent_home_visit_date

    @property
    def next_recommended_home_visit_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.next_recommended_home_visit_date

    @property
    def most_recent_treatment_collateral_contact_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.most_recent_treatment_collateral_contact_date

    @property
    def next_recommended_treatment_collateral_contact_date(self) -> Optional[date]:
        if not self.case_compliance:
            return None
        return self.case_compliance.next_recommended_treatment_collateral_contact_date

    @property
    def home_visit_count(self) -> Optional[int]:
        if not self.case_compliance:
            return None
        return self.case_compliance.home_visit_count

    @property
    def recommended_supervision_downgrade_level(
        self,
    ) -> Optional[StateSupervisionLevel]:
        if not self.case_compliance:
            return None
        return self.case_compliance.recommended_supervision_downgrade_level

    @property
    def is_out_of_state_custodial_authority(self) -> bool:
        return self.custodial_authority is not None and self.custodial_authority in (
            StateCustodialAuthority.FEDERAL,
            StateCustodialAuthority.OTHER_COUNTRY,
            StateCustodialAuthority.OTHER_STATE,
        )


@attr.s(frozen=True)
class ProjectedSupervisionCompletionEvent(SupervisionEvent):
    """Models a month in which supervision was projected to complete.

    Describes whether or not the supervision was successfully completed or not, as well
    as other details about the time on supervision.
    """

    # Whether or not the supervision was completed successfully
    successful_completion: bool = attr.ib(default=True)

    # Whether or not there were any incarceration admissions during the sentence
    incarcerated_during_sentence: bool = attr.ib(default=True)

    # Length of time served on the supervision sentence, in days
    sentence_days_served: int = attr.ib(default=None)


@attr.s(frozen=True)
class SupervisionStartEvent(SupervisionEvent, InPopulationMixin):
    """Models a day on which supervision started."""

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
class SupervisionTerminationEvent(
    SupervisionEvent, ViolationHistoryMixin, InPopulationMixin
):
    """Models a day on which supervision was terminated.

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
