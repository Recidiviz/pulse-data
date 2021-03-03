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
"""Program metrics we calculate."""

from datetime import date
from typing import Optional, Dict, Any, cast

import attr

from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    PersonLevelMetric,
    RecidivizMetricType,
    AssessmentMetric,
    SupervisionLocationMetric,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType


class ProgramMetricType(RecidivizMetricType):
    """The type of program metrics."""

    PROGRAM_PARTICIPATION = "PROGRAM_PARTICIPATION"
    PROGRAM_REFERRAL = "PROGRAM_REFERRAL"


@attr.s
class ProgramMetric(RecidivizMetric, PersonLevelMetric):
    """Models a single program metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """

    # Required characteristics

    # The type of ProgramMetric
    metric_type: ProgramMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Optional characteristics

    # Month
    month: Optional[int] = attr.ib(default=None)

    # Program ID
    program_id: str = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(
        metric_key: Dict[str, Any], job_id: str
    ) -> Optional["ProgramMetric"]:
        """Builds a ProgramMetric object from the given
        arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key["job_id"] = job_id
        metric_key["created_on"] = date.today()

        program_metric = cast(
            ProgramMetric, ProgramMetric.build_from_dictionary(metric_key)
        )

        return program_metric


@attr.s
class ProgramReferralMetric(ProgramMetric, AssessmentMetric, SupervisionLocationMetric):
    """Subclass of ProgramMetric that contains program referral information."""

    # Required characteristics

    # The type of ProgramMetric
    metric_type: ProgramMetricType = attr.ib(
        init=False, default=ProgramMetricType.PROGRAM_REFERRAL
    )

    # The date on which the referral took place
    date_of_referral: date = attr.ib(default=None)

    # Optional characteristics

    # Supervision Type
    # TODO(#2891): Make this of type StateSupervisionPeriodSupervisionType
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    # Program participation status
    participation_status: Optional[StateProgramAssignmentParticipationStatus] = attr.ib(
        default=None
    )

    # External ID of the officer who was supervising the person described by this metric
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(
        metric_key: Dict[str, Any], job_id: str
    ) -> Optional["ProgramReferralMetric"]:
        """Builds a ProgramReferralMetric object from the given
        arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key["job_id"] = job_id
        metric_key["created_on"] = date.today()

        program_metric = cast(
            ProgramReferralMetric,
            ProgramReferralMetric.build_from_dictionary(metric_key),
        )

        return program_metric


@attr.s
class ProgramParticipationMetric(ProgramMetric):
    """Subclass of ProgramMetric that contains program participation information."""

    # Required characteristics

    # The type of ProgramMetric
    metric_type: ProgramMetricType = attr.ib(
        init=False, default=ProgramMetricType.PROGRAM_PARTICIPATION
    )

    # Date of active participation
    date_of_participation: date = attr.ib(default=None)

    # Whether the date_of_participation was the first day the person participated in the program
    is_first_day_in_program: Optional[bool] = attr.ib(default=None)

    # Optional characteristics

    # Program location ID
    program_location_id: Optional[str] = attr.ib(default=None)

    # Supervision Type
    # TODO(#2891): Make this of type StateSupervisionPeriodSupervisionType
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(
        metric_key: Dict[str, Any], job_id: str
    ) -> Optional["ProgramParticipationMetric"]:
        """Builds a ProgramParticipationMetric object from the given arguments."""

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key["job_id"] = job_id
        metric_key["created_on"] = date.today()

        program_metric = cast(
            ProgramParticipationMetric,
            ProgramParticipationMetric.build_from_dictionary(metric_key),
        )

        return program_metric
