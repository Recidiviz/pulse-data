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

"""Creates BQ views to calculate all metrics by month for the jails part of Justice Counts."""

from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.calculator.query.justice_counts.views import metric_by_month
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_STATE_CODE_AGGREGATION = metric_by_month.Aggregation(
    dimension=manual_upload.State, comprehensive=False
)
_COUNTY_CODE_AGGREGATION = metric_by_month.Aggregation(
    dimension=manual_upload.County, comprehensive=False
)

METRICS = [
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.POPULATION,
        filtered_dimensions=[manual_upload.PopulationType.JAIL],
        aggregated_dimensions={
            "state_code": _STATE_CODE_AGGREGATION,
            "county_code": _COUNTY_CODE_AGGREGATION,
        },
        output_name="POPULATION_JAIL",
    ),
]


class JailsMetricsByMonthBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """Collects all views required for jail calculations"""

    def __init__(self) -> None:
        self.metric_builders: List[SimpleBigQueryViewBuilder] = []
        unified_query_select_clauses = []
        for metric in METRICS:
            view_builder = metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
                metric_to_calculate=metric,
            )
            # TODO(#6015): Split the CalculatedMetricByMonthViewBuilder to
            # support more composition of parts and alternative outputs instead of this
            # hackery.
            unified_query_select_clauses.append(
                f"SELECT * EXCEPT (source_name, source_url, report_name, raw_source_categories), "
                f"NULL as percentage_covered_county, NULL as percentage_covered_population "
                f"FROM `{{project_id}}.{{calculation_dataset}}.{view_builder.view_id}_materialized`"
            )
            self.metric_builders.append(view_builder)

        self.unified_builder = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_DASHBOARD_DATASET,
            view_id="unified_jails_metrics_by_month",
            view_query_template=" UNION ALL ".join(unified_query_select_clauses) + ";",
            description="Unified view of all calculated jails metrics by month",
            calculation_dataset=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        )

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        return self.metric_builders + [self.unified_builder]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = JailsMetricsByMonthBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
