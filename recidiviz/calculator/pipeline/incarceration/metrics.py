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

from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason


class IncarcerationMetricType(Enum):
    """The type of incarceration metrics."""

    ADMISSION = 'ADMISSION'
    POPULATION = 'POPULATION'
    RELEASE = 'RELEASE'


@attr.s
class IncarcerationMetric(RecidivizMetric):
    """Models a single incarceration metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
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
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['IncarcerationMetric']:
        """Builds a IncarcerationMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationMetric,
                                    IncarcerationMetric.
                                    build_from_dictionary(metric_key))

        return incarceration_metric


@attr.s
class IncarcerationPopulationMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains incarceration population
    counts at the end of the month."""
    # Required characteristics

    # Population count
    count: int = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['IncarcerationPopulationMetric']:
        """Builds a IncarcerationPopulationMetric object from the
         given arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationPopulationMetric,
                                    IncarcerationPopulationMetric.
                                    build_from_dictionary(metric_key))

        return incarceration_metric


@attr.s
class IncarcerationAdmissionMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains admission counts."""
    # Required characteristics

    # Admission count
    count: int = attr.ib(default=None)

    # Admission reason
    admission_reason: \
        StateIncarcerationPeriodAdmissionReason = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['IncarcerationAdmissionMetric']:
        """Builds a IncarcerationAdmissionMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationAdmissionMetric,
                                    IncarcerationAdmissionMetric.
                                    build_from_dictionary(metric_key))

        return incarceration_metric


@attr.s
class IncarcerationReleaseMetric(IncarcerationMetric):
    """Subclass of IncarcerationMetric that contains release counts."""
    # Required characteristics

    # Release count
    count: int = attr.ib(default=None)

    # Release reason
    release_reason: \
        StateIncarcerationPeriodReleaseReason = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['IncarcerationReleaseMetric']:
        """Builds a IncarcerationReleaseMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        incarceration_metric = cast(IncarcerationReleaseMetric,
                                    IncarcerationReleaseMetric.
                                    build_from_dictionary(metric_key))

        return incarceration_metric
