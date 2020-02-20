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
"""Program metrics we calculate."""

from datetime import date
from enum import Enum
from typing import Optional, Dict, Any, cast

import attr

from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric, PersonLevelMetric
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType


class ProgramMetricType(Enum):
    """The type of program metrics."""

    REFERRAL = 'REFERRAL'


@attr.s
class ProgramMetric(RecidivizMetric, PersonLevelMetric):
    """Models a single program metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """
    # Required characteristics

    # Year
    year: int = attr.ib(default=None)

    # Optional characteristics

    # Month
    month: Optional[int] = attr.ib(default=None)

    # The number of months this metric describes, starting with the month
    # of the metric and going back in time
    metric_period_months: Optional[int] = attr.ib(default=1)

    # Program ID
    program_id: str = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['ProgramMetric']:
        """Builds a ProgramMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        program_metric = cast(ProgramMetric,
                              ProgramMetric.
                              build_from_dictionary(metric_key))

        return program_metric


@attr.s
class ProgramReferralMetric(ProgramMetric):
    """Subclass of ProgramMetric that contains program referral counts."""
    # Required characteristics

    # Referral count
    count: int = attr.ib(default=None)

    # Optional characteristics

    # Supervision Type
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    # Assessment score of the people this metric describes
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # External ID of the officer who was supervising the people described by
    # this metric
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the district of the officer that was supervising the
    # people described by this metric
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['ProgramReferralMetric']:
        """Builds a ProgramReferralMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        program_metric = cast(ProgramReferralMetric,
                              ProgramReferralMetric.
                              build_from_dictionary(metric_key))

        return program_metric
