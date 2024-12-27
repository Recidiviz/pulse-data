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
"""Helper function to create view builders for vitals single day aggregated metrics"""

from dateutil.relativedelta import relativedelta
from more_itertools import one

from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE,
    AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED,
    AVG_DAILY_POPULATION_CONTACT_OVERDUE,
    AVG_DAILY_POPULATION_CONTACT_REQUIRED,
    AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import VITALS_REPORT_DATASET
from recidiviz.common.date import current_datetime_us_eastern
from recidiviz.tools.analyst.aggregated_metrics_utils import (
    get_legacy_custom_aggregated_metrics_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent


def get_view_builders(
    time_interval_length_in_days: int,
) -> list[SimpleBigQueryViewBuilder]:
    """Creates view builders for vitals aggregated metrics calculated daily across the given length
    of time."""
    current_datetime = current_datetime_us_eastern()
    view_builders = []
    for unit_of_analysis_type in [
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        MetricUnitOfAnalysisType.STATE_CODE,
    ]:
        sld_metric = one(
            metric
            for metric in AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION
            if metric.name == "avg_population_task_eligible_supervision_level_downgrade"
        )
        # TODO(#35910): Migrate to use an optimized custom metrics template builder
        #  once it exists.
        agg_metrics_query_template = get_legacy_custom_aggregated_metrics_query_template(
            metrics=[
                AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED,
                AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE,
                AVG_DAILY_POPULATION_CONTACT_REQUIRED,
                AVG_DAILY_POPULATION_CONTACT_OVERDUE,
                AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE,
                sld_metric,
            ],
            unit_of_analysis_type=unit_of_analysis_type,
            population_type=MetricPopulationType.SUPERVISION,
            time_interval_unit=MetricTimePeriod.DAY,
            # How long to calculate the averages over. For example, if
            # time_interval_length_in_days is 30, we calculate a 30 day rate. If
            # time_interval_length_in_days is 1, we have a point-in-time count.
            time_interval_length=time_interval_length_in_days,
            min_end_date=current_datetime - relativedelta(days=30),
            max_end_date=current_datetime,
            # How often to calculate metrics. Since the rolling period is set to 1 DAY, we
            # calculate values for each day in the time period.
            rolling_period_unit=MetricTimePeriod.DAY,
            rolling_period_length=1,
        )
        query_template = f"""
WITH agg_metrics AS (
{fix_indent(agg_metrics_query_template, indent_level=4)}
)
SELECT * FROM agg_metrics
WHERE state_code IN ("US_IX", "US_ND")
"""
        view_id = f"supervision_{unit_of_analysis_type.short_name}_{f'{time_interval_length_in_days}_' if time_interval_length_in_days != 1 else ''}day_aggregated_metrics"
        view_description = f"""
        Vitals metrics for the {MetricPopulationType.SUPERVISION.population_name_short} population,
        over a {time_interval_length_in_days} time interval, disaggregated by
        {unit_of_analysis_type.short_name}, and calculated for each day.

        All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
        """
        view_builder = SimpleBigQueryViewBuilder(
            dataset_id=VITALS_REPORT_DATASET,
            view_query_template=query_template,
            view_id=view_id,
            description=view_description,
            should_materialize=True,
            clustering_fields=["state_code"],
        )
        view_builders.append(view_builder)
    return view_builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for vb in get_view_builders(time_interval_length_in_days=1):
            vb.build_and_print()
        for vb in get_view_builders(time_interval_length_in_days=30):
            vb.build_and_print()
