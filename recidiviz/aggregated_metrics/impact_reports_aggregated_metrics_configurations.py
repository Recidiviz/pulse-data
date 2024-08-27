# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines AggregatedMetric objects for impact reports"""

from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    DEDUPED_TASK_COMPLETION_EVENT_VB,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    DailyAvgSpanCountMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_type import (
    SpanType,
)
from recidiviz.workflows.types import WorkflowsSystemType


AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Almost Eligible, {b.task_title}",
        description=f"Average daily count of residents almost eligible for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.TASK_ELIGIBILITY_SESSION,
                span_conditions_dict={
                    "is_almost_eligible": ["true"],
                    "task_type": [b.task_type_name],
                },
            )
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Almost Eligible, {b.task_title}",
        description="Average daily count of residents almost eligible for task of "
        f"type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.TASK_ELIGIBILITY_SESSION,
                span_conditions_dict={
                    "is_almost_eligible": ["true"],
                    "task_type": [b.task_type_name],
                },
            )
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]
