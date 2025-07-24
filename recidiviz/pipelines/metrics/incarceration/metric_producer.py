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
from typing import Dict, List, Sequence, Type

from recidiviz.pipelines.metrics.base_metric_producer import BaseMetricProducer
from recidiviz.pipelines.metrics.incarceration.events import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
)
from recidiviz.pipelines.metrics.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationCommitmentFromSupervisionMetric,
    IncarcerationMetric,
    IncarcerationMetricType,
    IncarcerationReleaseMetric,
)


class IncarcerationMetricProducer(
    BaseMetricProducer[
        IncarcerationEvent,
        Sequence[IncarcerationEvent],
        IncarcerationMetricType,
        IncarcerationMetric,
    ]
):
    """Calculates IncarcerationMetrics from IncarcerationEvents."""

    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = IncarcerationMetric  # type: ignore

    @property
    def result_class_to_metric_classes_mapping(
        self,
    ) -> Dict[Type[IncarcerationEvent], List[Type[IncarcerationMetric]]]:
        return {
            IncarcerationStandardAdmissionEvent: [IncarcerationAdmissionMetric],
            IncarcerationCommitmentFromSupervisionAdmissionEvent: [
                IncarcerationAdmissionMetric,
                IncarcerationCommitmentFromSupervisionMetric,
            ],
            IncarcerationReleaseEvent: [IncarcerationReleaseMetric],
        }
