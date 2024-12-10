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
"""Aggregated metrics config objects for our standard set of aggregated metrics."""
from recidiviz.aggregated_metrics.aggregated_metric_collection_config import (
    AggregatedMetricsCollection,
)
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.standard_deployed_metrics_by_population import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.standard_deployed_unit_of_analysis_types_by_population_type import (
    UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
)

STANDARD_METRICS_YEARS_TRACKED = 7

STANDARD_TIME_PERIODS = [
    MetricTimePeriodConfig.monthly_year_periods(
        lookback_months=STANDARD_METRICS_YEARS_TRACKED * 12
    ),
    MetricTimePeriodConfig.monthly_quarter_periods(
        lookback_months=STANDARD_METRICS_YEARS_TRACKED * 12
    ),
    MetricTimePeriodConfig.month_periods(
        lookback_months=STANDARD_METRICS_YEARS_TRACKED * 12
    ),
]

STANDARD_COLLECTION_CONFIG = AggregatedMetricsCollection.build(
    output_dataset_id=AGGREGATED_METRICS_DATASET_ID,
    time_periods=STANDARD_TIME_PERIODS,
    unit_of_analysis_types_by_population_type=UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
    metrics_by_population_type=METRICS_BY_POPULATION_TYPE,
)
