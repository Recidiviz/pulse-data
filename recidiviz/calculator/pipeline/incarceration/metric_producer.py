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
"""Calculates IncarcerationMetrics from IncarcerationEvents.

This contains the core logic for calculating incarceration metrics on a person-by-person
basis. It transforms IncarcerationEvents into IncarcerationMetrics.
"""
from typing import List

from recidiviz.calculator.pipeline.base_metric_producer import BaseMetricProducer
from recidiviz.calculator.pipeline.incarceration.incarceration_event import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationMetric,
    IncarcerationMetricType,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)


class IncarcerationMetricProducer(
    BaseMetricProducer[
        List[IncarcerationEvent], IncarcerationMetricType, IncarcerationMetric
    ]
):
    """Calculates IncarcerationMetrics from IncarcerationEvents."""

    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = IncarcerationMetric  # type: ignore
        self.event_to_metric_classes = {
            IncarcerationStandardAdmissionEvent: [IncarcerationAdmissionMetric],
            IncarcerationCommitmentFromSupervisionAdmissionEvent: [
                IncarcerationAdmissionMetric,
                IncarcerationCommitmentFromSupervisionMetric,
            ],
            IncarcerationStayEvent: [IncarcerationPopulationMetric],
            IncarcerationReleaseEvent: [IncarcerationReleaseMetric],
        }
