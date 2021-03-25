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
from typing import List, Dict, Optional, Type

from recidiviz.calculator.pipeline.program.metrics import (
    ProgramMetricType,
    ProgramMetric,
    ProgramParticipationMetric,
    ProgramReferralMetric,
)
from recidiviz.calculator.pipeline.program.program_event import (
    ProgramEvent,
    ProgramReferralEvent,
    ProgramParticipationEvent,
)
from recidiviz.calculator.pipeline.utils.calculator_utils import (
    produce_standard_metrics,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.persistence.entity.state.entities import StatePerson

EVENT_TO_METRIC_TYPES: Dict[Type[ProgramEvent], ProgramMetricType] = {
    ProgramReferralEvent: ProgramMetricType.PROGRAM_REFERRAL,
    ProgramParticipationEvent: ProgramMetricType.PROGRAM_PARTICIPATION,
}

EVENT_TO_METRIC_CLASSES: Dict[Type[ProgramEvent], Type[ProgramMetric]] = {
    ProgramReferralEvent: ProgramReferralMetric,
    ProgramParticipationEvent: ProgramParticipationMetric,
}


def produce_program_metrics(
    person: StatePerson,
    program_events: List[ProgramEvent],
    metric_inclusions: Dict[ProgramMetricType, bool],
    calculation_end_month: Optional[str],
    calculation_month_count: int,
    person_metadata: PersonMetadata,
    pipeline_job_id: str,
) -> List[ProgramMetric]:
    """Transforms ProgramEvents and a StatePerson into metric combinations.

    Takes in a StatePerson and all of her ProgramEvents and returns a list of
    ProgramMetrics by translating a particular interaction with a program into a
    program metric.

    Args:
        person: the StatePerson
        program_events: A list of ProgramEvents for the given StatePerson.
        metric_inclusions: A dictionary where the keys are each ProgramMetricType, and the values are boolean
            flags for whether or not to include that metric type in the calculations
        calculation_end_month: The year and month in YYYY-MM format of the last month for which metrics should be
            calculated. If unset, ends with the current month.
        calculation_month_count: The number of months (including the month of the calculation_end_month) to
            limit the monthly calculation output to. If set to -1, does not limit the calculations.
        person_metadata: Contains information about the StatePerson that is necessary for the metrics.
        pipeline_job_id: The job_id of the pipeline that is currently running.

    Returns:
        A list of ProgramMetrics.
    """
    metrics = produce_standard_metrics(
        pipeline="program",
        person=person,
        identifier_events=program_events,
        metric_inclusions=metric_inclusions,
        calculation_end_month=calculation_end_month,
        calculation_month_count=calculation_month_count,
        person_metadata=person_metadata,
        event_to_metric_types=EVENT_TO_METRIC_TYPES,
        event_to_metric_classes=EVENT_TO_METRIC_CLASSES,
        pipeline_job_id=pipeline_job_id,
    )

    program_metrics: List[ProgramMetric] = []

    for metric in metrics:
        if not isinstance(metric, ProgramMetric):
            raise ValueError(
                f"Unexpected metric type {type(metric)}."
                f" All metrics should be ProgramMetrics."
            )
        program_metrics.append(metric)

    return program_metrics
