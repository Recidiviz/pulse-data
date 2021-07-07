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
"""Base class for a metric producer for a calculation pipeline."""

import abc
from typing import Dict, Generic, List, Optional, Type, TypeVar

import attr

from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.utils.calculator_utils import (
    produce_standard_metrics,
)
from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.persistence.entity.state.entities import StatePerson

IdentifierEventResultT = TypeVar("IdentifierEventResultT")
RecidivizMetricTypeT = TypeVar("RecidivizMetricTypeT", bound=RecidivizMetricType)
RecidivizMetricT = TypeVar("RecidivizMetricT", bound=RecidivizMetric)


@attr.s
class BaseMetricProducer(
    abc.ABC, Generic[IdentifierEventResultT, RecidivizMetricTypeT, RecidivizMetricT]
):
    """Base class for a metric producer for a calculation pipeline."""

    metric_class: Type[RecidivizMetricT] = attr.ib()
    event_to_metric_classes: Dict[
        Type[IdentifierEvent], List[Type[RecidivizMetricT]]
    ] = attr.ib()

    def produce_metrics(
        self,
        person: StatePerson,
        identifier_events: IdentifierEventResultT,
        metric_inclusions: Dict[RecidivizMetricTypeT, bool],
        person_metadata: PersonMetadata,
        pipeline_type: PipelineType,
        pipeline_job_id: str,
        calculation_end_month: Optional[str] = None,
        calculation_month_count: int = -1,
    ) -> List[RecidivizMetricT]:
        """Transforms the events and a StatePerson into RecidivizMetrics.
        Args:
            person: the StatePerson
            identifier_events: A list of IdentifierEvents for the given StatePerson.
            metric_inclusions: A dictionary where the keys are each Metric type and the values are boolean
                flags for whether or not to include that metric type in the calculations.
            calculation_end_month: The year and month in YYYY-MM format of the last month for which metrics should be
                calculated. If unset, ends with the current month.
            calculation_month_count: The number of months (including the month of the calculation_month_upper_bound) to
                limit the monthly calculation output to. If set to -1, does not limit the calculations.
            person_metadata: Contains information about the StatePerson that is necessary for the metrics.
            pipeline_job_id: The job_id of the pipeline that is currently running.
            pipeline_type: The PipelineType used to name the set of metrics
        Returns:
            A list of RecidivizMetrics
        """
        metrics = produce_standard_metrics(
            pipeline=pipeline_type.value.lower(),
            person=person,
            identifier_events=identifier_events,  # type: ignore
            metric_inclusions=metric_inclusions,
            calculation_end_month=calculation_end_month,
            calculation_month_count=calculation_month_count,
            person_metadata=person_metadata,
            event_to_metric_classes=self.event_to_metric_classes,  # type: ignore
            pipeline_job_id=pipeline_job_id,
        )

        metrics_of_class: List[RecidivizMetricT] = []

        for metric in metrics:
            if not isinstance(metric, self.metric_class):
                raise ValueError(
                    f"Unexpected metric type {type(metric)}."
                    f" All metrics should be of type {self.metric_class}."
                )
            metrics_of_class.append(metric)

        return metrics_of_class
