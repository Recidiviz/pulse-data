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

from datetime import date
from enum import Enum
from typing import Optional, Dict, Any, cast

import attr

from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric, PersonLevelMetric
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType


class IncarcerationMetricType(Enum):
    """The type of incarceration metrics."""

    ADMISSION = 'ADMISSION'
    POPULATION = 'POPULATION'
    RELEASE = 'RELEASE'


@attr.s
class IncarcerationMetric(RecidivizMetric, PersonLevelMetric):
    """Models a single incarceration metric.

    Contains all of the identifying characteristics of the metric, including required characteristics for normalization
    as well as optional characteristics for slicing the data.
    """
    # Required characteristics

    # Year
    year: int = attr.ib(default=None)

    # Month
    month: int = attr.ib(default=None)

    # The number of months this metric describes, starting with the month
    # of the metric and going back in time
    metric_period_months: Optional[int] = attr.ib(default=1)

    # Optional characteristics

    # Facility
    facility: Optional[str] = attr.ib(default=None)

    # County of residence
    county_of_residence: Optional[str] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> Optional['IncarcerationMetric']:
        """Builds a IncarcerationMetric object from the given arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationMetric,
                                    IncarcerationMetric.build_from_dictionary(metric_key))

        return incarceration_metric


@attr.s
class IncarcerationPopulationMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains incarceration population counts at the end of the month."""
    # Required characteristics

    # Population count
    count: int = attr.ib(default=None)

    # Optional characteristics

    # The most serious offense statute connected to the sentence group of the incarceration period from which
    # this stay event is derived
    most_serious_offense_statute: Optional[str] = attr.ib(default=None)

    # Admission reason for the incarceration period that overlaps with relevant day.
    admission_reason: StateIncarcerationPeriodAdmissionReason = attr.ib(default=None)

    # Admission reason raw text
    admission_reason_raw_text: str = attr.ib(default=None)

    # Supervision type at the time of admission, if any.
    supervision_type_at_admission: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> \
            Optional['IncarcerationPopulationMetric']:
        """Builds a IncarcerationPopulationMetric object from the given arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationPopulationMetric,
                                    IncarcerationPopulationMetric.build_from_dictionary(metric_key))

        return incarceration_metric


@attr.s
class IncarcerationAdmissionMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains admission counts."""
    # Required characteristics

    # Admission count
    count: int = attr.ib(default=None)

    # Most relevant admission reason for a continuous stay in prison. For example, in some states, if the initial
    # incarceration period has an admission reason of TEMPORARY_CUSTODY, the admission reason is drawn from the
    # subsequent admission period, if present.
    admission_reason: StateIncarcerationPeriodAdmissionReason = attr.ib(default=None)

    # Admission reason raw text
    admission_reason_raw_text: str = attr.ib(default=None)

    # Specialized purpose for incarceration
    specialized_purpose_for_incarceration: Optional[StateSpecializedPurposeForIncarceration] = attr.ib(default=None)

    # Supervision type at the time of admission, if any.
    supervision_type_at_admission: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> \
            Optional['IncarcerationAdmissionMetric']:
        """Builds a IncarcerationAdmissionMetric object from the given arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationAdmissionMetric,
                                    IncarcerationAdmissionMetric.build_from_dictionary(metric_key))

        return incarceration_metric


@attr.s
class IncarcerationReleaseMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains release counts."""
    # Required characteristics

    # Release count
    count: int = attr.ib(default=None)

    # Release reason
    release_reason: StateIncarcerationPeriodReleaseReason = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any], job_id: str) -> \
            Optional['IncarcerationReleaseMetric']:
        """Builds a IncarcerationReleaseMetric object from the given arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationReleaseMetric,
                                    IncarcerationReleaseMetric.build_from_dictionary(metric_key))

        return incarceration_metric
