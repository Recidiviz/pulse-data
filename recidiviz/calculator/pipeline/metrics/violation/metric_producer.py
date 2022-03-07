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
"""Calculates ViolationMetrics from ViolationEvents on a person-by-person basis."""
from typing import Dict, List, Type

from recidiviz.calculator.pipeline.metrics.base_metric_producer import (
    BaseMetricProducer,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.calculator.pipeline.metrics.violation.events import (
    ViolationEvent,
    ViolationWithResponseEvent,
)
from recidiviz.calculator.pipeline.metrics.violation.metrics import (
    ViolationMetric,
    ViolationMetricType,
    ViolationWithResponseMetric,
)

EVENT_TO_METRIC_CLASSES: Dict[
    Type[ViolationEvent], List[Type[RecidivizMetric[ViolationMetricType]]]
] = {ViolationWithResponseEvent: [ViolationWithResponseMetric]}


class ViolationMetricProducer(
    BaseMetricProducer[List[ViolationEvent], ViolationMetricType, ViolationMetric]
):
    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = ViolationMetric  # type: ignore
        self.event_to_metric_classes = {
            ViolationWithResponseEvent: [ViolationWithResponseMetric]
        }
