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
from typing import Dict, List, Optional, Type

from recidiviz.calculator.pipeline.utils.calculator_utils import (
    produce_standard_metrics,
)
from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.calculator.pipeline.violation.metrics import (
    ViolationMetric,
    ViolationMetricType,
    ViolationWithResponseMetric,
)
from recidiviz.calculator.pipeline.violation.violation_event import (
    ViolationEvent,
    ViolationWithResponseDecisionEvent,
)
from recidiviz.persistence.entity.state.entities import StatePerson

EVENT_TO_METRIC_CLASSES: Dict[
    Type[ViolationEvent], List[Type[RecidivizMetric[ViolationMetricType]]]
] = {ViolationWithResponseDecisionEvent: [ViolationWithResponseMetric]}


def produce_violation_metrics(
    person: StatePerson,
    violation_events: List[ViolationEvent],
    metric_inclusions: Dict[ViolationMetricType, bool],
    calculation_end_month: Optional[str],
    calculation_month_count: int,
    person_metadata: PersonMetadata,
    pipeline_job_id: str,
) -> List[ViolationMetric]:
    """Transforms ViolationEvents and a StatePerson into metrics.

    Takes in a StatePerson and all of their ViolationEvents and returns a list of
    ViolationMetrics by translating a particular interaction with a violation into a
    violation metric.

    Args:
        person: the StatePerson
        violation_events: A list of ViolationEvents for the given StatePerson.
        metric_inclusions: A dictionary where the keys are each ViolationMetricType, and the values are boolean
            flags for whether or not to include that metric type in the calculations
        calculation_end_month: The year and month in YYYY-MM format of the last month for which metrics should be
            calculated. If unset, ends with the current month.
        calculation_month_count: The number of months (including the month of the calculation_end_month) to
            limit the monthly calculation output to. If set to -1, does not limit the calculations.
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.
        pipeline_job_id: The job_id of the pipeline that is currently running.

    Returns:
        A list of ViolationMetrics.
    """
    metrics = produce_standard_metrics(
        pipeline="violation",
        person=person,
        identifier_events=violation_events,
        metric_inclusions=metric_inclusions,
        calculation_end_month=calculation_end_month,
        calculation_month_count=calculation_month_count,
        person_metadata=person_metadata,
        event_to_metric_classes=EVENT_TO_METRIC_CLASSES,
        pipeline_job_id=pipeline_job_id,
    )

    violation_metrics: List[ViolationMetric] = []

    for metric in metrics:
        if not isinstance(metric, ViolationMetric):
            raise ValueError(
                f"Unexpected metric type {type(metric)}."
                f" All metrics should be ProgramMetrics."
            )
        violation_metrics.append(metric)

    return violation_metrics
