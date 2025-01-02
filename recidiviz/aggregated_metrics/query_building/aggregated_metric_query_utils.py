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
"""Utils shared by various parts aggregated metrics query building."""
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)

AggregatedMetricClassType = (
    type[PeriodSpanAggregatedMetric]
    | type[PeriodEventAggregatedMetric]
    | type[AssignmentSpanAggregatedMetric]
    | type[AssignmentEventAggregatedMetric]
)


def metric_group_by_columns(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
) -> list[str]:
    """This is the set of columns we group by to produce aggregated metrics for the
    given |unit_of_analysis|."""
    unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)
    return [
        *unit_of_analysis.primary_key_columns,
        *MetricTimePeriodConfig.query_output_columns(),
    ]


# TODO(#35914): Remove this function once aggregated metrics ship for all metric classes
def is_metric_class_supported_by_optimized_format(
    metric_class: AggregatedMetricClassType,  # pylint: disable=unused-argument
) -> bool:
    return True
