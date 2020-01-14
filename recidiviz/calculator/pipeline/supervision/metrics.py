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

from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType


class SupervisionMetricType(Enum):
    """The type of supervision metrics."""

    POPULATION = 'POPULATION'
    REVOCATION = 'REVOCATION'
    SUCCESS = 'SUCCESS'


@attr.s
class SupervisionMetric(RecidivizMetric):
    """Models a single supervision metric.

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

    # Supervision Type
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)

    # External ID of the officer who was supervising the people described by
    # this metric
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the district of the officer that was supervising the
    # people described by this metric
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['SupervisionMetric']:
        """Builds a SupervisionMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionMetric,
                                  SupervisionMetric.
                                  build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionPopulationMetric(SupervisionMetric):
    """Subclass of SupervisionMetric that contains supervision population
    counts."""
    # Required characteristics

    # Population count
    count: int = attr.ib(default=None)

    # Optional characteristics

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['SupervisionPopulationMetric']:
        """Builds a SupervisionPopulationMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionPopulationMetric,
                                  SupervisionPopulationMetric.
                                  build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionRevocationMetric(SupervisionMetric):
    """Subclass of SupervisionMetric that contains supervision revocation
    counts."""
    # Required characteristics

    # Revocation count
    count: int = attr.ib(default=None)

    # Optional characteristics

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)

    # The StateSupervisionViolationResponseRevocationType enum for the type of
    # revocation of supervision that this metric describes
    revocation_type: Optional[StateSupervisionViolationResponseRevocationType] \
        = attr.ib(default=None)

    # StateSupervisionViolationType enum for the type of violation that
    # eventually caused the revocation of supervision
    source_violation_type: Optional[StateSupervisionViolationType] = \
        attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['SupervisionRevocationMetric']:
        """Builds a SupervisionRevocationMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionRevocationMetric,
                                  SupervisionRevocationMetric.
                                  build_from_dictionary(metric_key))

        return supervision_metric


@attr.s
class SupervisionSuccessMetric(SupervisionMetric):
    """Subclass of SupervisionMetric that contains supervision success and
    failure counts."""
    # Required characteristics

    # Number of successful completions
    successful_completion_count: int = attr.ib(default=None)

    # Total number of projected completions
    projected_completion_count: int = attr.ib(default=None)

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['SupervisionSuccessMetric']:
        """Builds a SupervisionSuccessMetric object from the given
         arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        supervision_metric = cast(SupervisionSuccessMetric,
                                  SupervisionSuccessMetric.
                                  build_from_dictionary(metric_key))

        return supervision_metric
