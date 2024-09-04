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

from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import IMPACT_REPORTS_DATASET_ID
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.tools.analyst.aggregated_metrics_utils import (
    get_custom_aggregated_metrics_query_template,
)

MIN_DATE = datetime.datetime(2023, 1, 1)
MAX_DATE = datetime.datetime.today()


def get_impact_reports_aggregated_metrics_view_builders(
    metrics_by_time_period: dict[MetricTimePeriod, list[AggregatedMetric]]
) -> List[SimpleBigQueryViewBuilder]:
    """
    Collects all aggregated metrics view builders for impact reports at the state and day level
    """

    view_builders = []
    for time_period, metrics in metrics_by_time_period.items():
        query_template = get_custom_aggregated_metrics_query_template(
            metrics=metrics,
            unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE,
            population_type=MetricPopulationType.JUSTICE_INVOLVED,
            time_interval_unit=time_period,
            time_interval_length=1,
            min_date=MIN_DATE,
            max_date=MAX_DATE,
            # The rolling_period parameters let us get metrics for any time interval, each day. For
            # example, if time_period is MONTH, then we can get month-long metrics for each day of
            # the month instead of the default, which would be to do month-long metrics on just the
            # first of the month. This allows us to generate reports for periods that don't fall
            # neatly along month boundaries.
            rolling_period_unit=MetricTimePeriod.DAY,
            rolling_period_length=1,
        )

        view_id = f"{MetricPopulationType.JUSTICE_INVOLVED.population_name_short}_{MetricUnitOfAnalysisType.STATE_CODE.short_name}_{time_period.value.lower()}_aggregated_metrics"

        view_description = f"""
        Metrics for the {MetricPopulationType.JUSTICE_INVOLVED.population_name_short} population calculated using 
        ad-hoc logic, disaggregated by {MetricUnitOfAnalysisType.STATE_CODE.short_name} and calculated for
        each day for the {time_period.value} period.

        All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
        """

        view_builders.append(
            SimpleBigQueryViewBuilder(
                dataset_id=IMPACT_REPORTS_DATASET_ID,
                view_query_template=query_template,
                view_id=view_id,
                description=view_description,
                should_materialize=True,
            )
        )

    return view_builders
