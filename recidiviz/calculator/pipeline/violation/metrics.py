# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Metrics related to violations and their responses and decisions."""
import abc

import attr

from recidiviz.calculator.pipeline.utils.event_utils import ViolationResponseMixin
from recidiviz.calculator.pipeline.utils.metric_utils import (
    PersonLevelMetric,
    RecidivizMetric,
    RecidivizMetricType,
)


class ViolationMetricType(RecidivizMetricType):
    """The type of violation metrics."""

    VIOLATION = "VIOLATION"


@attr.s
class ViolationMetric(RecidivizMetric[ViolationMetricType], PersonLevelMetric):
    """Models a single violation metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """

    # Required characteristics
    metric_type_cls = ViolationMetricType

    # The type of ViolationMetric
    metric_type: ViolationMetricType = attr.ib(default=None)

    # Year
    year: int = attr.ib(default=None)

    # Optional characteristics

    # Month
    month: int = attr.ib(default=None)

    # Violation Id
    supervision_violation_id: int = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class ViolationWithResponseMetric(ViolationMetric, ViolationResponseMixin):
    """Subclass of ViolationMetric that stores information about a violation and its response."""

    @classmethod
    def get_description(cls) -> str:
        return "TODO(#7563): Add ViolationWithResponseMetric description"

    metric_type: ViolationMetricType = attr.ib(
        init=False, default=ViolationMetricType.VIOLATION
    )
