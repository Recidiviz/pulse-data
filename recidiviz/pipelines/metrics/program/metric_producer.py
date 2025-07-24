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
"""Calculates program metrics from program events.

This contains the core logic for calculating program metrics on a person-by-person basis.
It transforms ProgramEvents into ProgramMetrics.
"""
from typing import Dict, List, Type

from recidiviz.pipelines.metrics.base_metric_producer import BaseMetricProducer
from recidiviz.pipelines.metrics.program.events import (
    ProgramEvent,
    ProgramParticipationEvent,
)
from recidiviz.pipelines.metrics.program.metrics import (
    ProgramMetric,
    ProgramMetricType,
    ProgramParticipationMetric,
)


class ProgramMetricProducer(
    BaseMetricProducer[
        ProgramEvent, List[ProgramEvent], ProgramMetricType, ProgramMetric
    ]
):
    """Calculates ProgramMetrics from ProgramEvents."""

    def __init__(self) -> None:
        # TODO(python/mypy#5374): Remove the ignore type when abstract class assignments are supported.
        self.metric_class = ProgramMetric  # type: ignore

    @property
    def result_class_to_metric_classes_mapping(
        self,
    ) -> Dict[Type[ProgramEvent], List[Type[ProgramMetric]]]:
        return {
            ProgramParticipationEvent: [ProgramParticipationMetric],
        }
