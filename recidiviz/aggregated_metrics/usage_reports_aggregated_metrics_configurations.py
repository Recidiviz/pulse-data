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
"""Defines AggregatedMetric objects for usage reports"""

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    DailyAvgSpanCountMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.observations.span_type import SpanType

AVG_DAILY_POPULATION_SURFACEABLE_NOT_MARKED_INELIGIBLE = DailyAvgSpanCountMetric(
    name="avg_population_surfaceable_and_not_marked_ineligible",
    display_name="Average Population: Surfaceable And Not Marked Ineligible",
    description="Average daily count of clients surfaceable and not marked ineligible for fully launched task types",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_surfaceable": ["true"],
            "marked_ineligible": ["false"],
            "task_type_is_fully_launched": ["true"],
        },
    ),
)
