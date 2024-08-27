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
"""Returns all aggregated metric view builders for specified populations and units of analysis related to impact reports"""

import datetime
from typing import List
from recidiviz.tools.analyst.aggregated_metrics_utils import (
    get_custom_aggregated_metrics_query_template,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.calculator.query.state.dataset_config import IMPACT_REPORTS_DATASET_ID


def get_impact_reports_aggregated_metrics_view_builders(
    metrics: List[AggregatedMetric],
) -> List[SimpleBigQueryViewBuilder]:
    """
    Collects all aggregated metrics view builders for impact reports at the state and day level
    """

    query_template = get_custom_aggregated_metrics_query_template(
        # The list of metrics we want to calculate
        metrics=metrics,
        # The level of aggregation we're interested in
        unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
        # The population for the analysis -- justice involved is a safe bet that captures everyone in the system
        population_type=MetricPopulationType.JUSTICE_INVOLVED,
        # The time of time interval at which we want to calculate these metrics
        time_interval_unit=MetricTimePeriod.DAY,
        # The number of time interval units -- we can keep this to 1
        time_interval_length=1,
        # The date range for metrics
        min_date=datetime.datetime(2023, 1, 1),
        max_date=datetime.datetime.today(),
    )

    view_id = f"{MetricPopulationType.JUSTICE_INVOLVED.population_name_short}_{MetricUnitOfAnalysisType.STATE_CODE.short_name}_aggregated_metrics"

    view_description = f"""
	Metrics for the {MetricPopulationType.JUSTICE_INVOLVED.population_name_short} population calculated using 
	ad-hoc logic, disaggregated by {MetricUnitOfAnalysisType.STATE_CODE.short_name}.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """

    view_builder = SimpleBigQueryViewBuilder(
        dataset_id=IMPACT_REPORTS_DATASET_ID,
        view_query_template=query_template,
        view_id=view_id,
        description=view_description,
        should_materialize=True,
    )

    return [view_builder]
