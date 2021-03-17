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

"""Creates BQ views to calculate all metrics by month for the corrections part of Justice Counts."""

from typing import List

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
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

METRICS = [
    # Admissions Metrics
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[manual_upload.AdmissionType.NEW_COMMITMENT],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_NEW_COMMITMENTS",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PAROLE,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PAROLE",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PAROLE,
            manual_upload.SupervisionViolationType.NEW_CRIME,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PAROLE_NEW_CRIME",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PAROLE,
            manual_upload.SupervisionViolationType.TECHNICAL,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PAROLE_TECHNICAL",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PROBATION,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PROBATION",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PROBATION,
            manual_upload.SupervisionViolationType.NEW_CRIME,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PROBATION_NEW_CRIME",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PROBATION,
            manual_upload.SupervisionViolationType.TECHNICAL,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PROBATION_TECHNICAL",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionViolationType.NEW_CRIME,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_ALL_SUPERVISION_NEW_CRIME",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionViolationType.TECHNICAL,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_ALL_SUPERVISION_TECHNICAL",
    ),
    # Population Metrics
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.POPULATION,
        filtered_dimensions=[
            manual_upload.PopulationType.SUPERVISION,
            manual_upload.SupervisionType.PAROLE,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="POPULATION_PAROLE",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.POPULATION,
        filtered_dimensions=[manual_upload.PopulationType.PRISON],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="POPULATION_PRISON",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.POPULATION,
        filtered_dimensions=[
            manual_upload.PopulationType.SUPERVISION,
            manual_upload.SupervisionType.PROBATION,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="POPULATION_PROBATION",
    ),
    # Release Metrics
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.RELEASES,
        filtered_dimensions=[manual_upload.ReleaseType.TO_SUPERVISION],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="RELEASES_SUPERVISION",
    ),
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.RELEASES,
        filtered_dimensions=[manual_upload.ReleaseType.COMPLETED],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="RELEASES_COMPLETED",
    ),
]

OUTPUT_VIEW_TEMPLATE = """
SELECT {aggregated_dimension_columns},
       '{metric_output_name}' as metric,
       EXTRACT(YEAR from start_of_month) as year,
       EXTRACT(MONTH from start_of_month) as month,
       DATE_SUB(time_window_end, INTERVAL 1 DAY) as date_reported,
       source.name as source_name,
       report.url as source_url,
       report_type as report_name,
       ARRAY(
         SELECT DISTINCT(collapsed_dimension_value)
         FROM UNNEST(collapsed_dimension_values) as collapsed_dimension_value
         ORDER BY collapsed_dimension_value
       ) as raw_source_categories,
       value as value,
       -- If there is no row at least a year back to compare to, the following columns will be NULL.
       EXTRACT(YEAR from compare_start_of_month) as compared_to_year,
       EXTRACT(MONTH from compare_start_of_month) as compared_to_month,
       value - compare_value as value_change,
       -- Note: This can return NULL in the case of divide by zero
       SAFE_DIVIDE((value - compare_value), compare_value) as percentage_change
FROM `{project_id}.{input_dataset}.{input_table}`
JOIN `{project_id}.{base_dataset}.source_materialized` source
  ON source_id = source.id
JOIN `{project_id}.{base_dataset}.report_materialized` report
  -- Just pick any report and use its URL
  ON report_ids[ORDINAL(1)] = report.id
"""


class CorrectionsOutputViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for creating corrections output from an input view."""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_to_calculate: metric_by_month.CalculatedMetricByMonth,
        input_view: BigQueryViewBuilder,
    ):
        aggregated_dimension_columns = ", ".join(
            metric_to_calculate.aggregated_dimensions
        )

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_to_calculate.view_prefix}_output",
            view_query_template=OUTPUT_VIEW_TEMPLATE,
            should_materialize=True,
            # Query Format Arguments
            description=f"{metric_to_calculate.output_name} dashboard output",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            metric_output_name=metric_to_calculate.output_name,
            aggregated_dimension_columns=aggregated_dimension_columns,
        )


def view_chain_for_metric(
    metric: metric_by_month.CalculatedMetricByMonth,
) -> List[SimpleBigQueryViewBuilder]:
    calculate_view_builder = metric_by_month.CalculatedMetricByMonthViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        metric_to_calculate=metric,
    )
    comparison_view_builder = metric_by_month.CompareToPriorYearViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        metric_to_calculate=metric,
        input_view=calculate_view_builder,
    )
    dimensions_to_columns_view_builder = metric_by_month.DimensionsToColumnsViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        metric_to_calculate=metric,
        input_view=comparison_view_builder,
    )
    output_view_builder = CorrectionsOutputViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        metric_to_calculate=metric,
        input_view=dimensions_to_columns_view_builder,
    )
    return [
        calculate_view_builder,
        comparison_view_builder,
        dimensions_to_columns_view_builder,
        output_view_builder,
    ]


class CorrectionsMetricsByMonthBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    def __init__(self) -> None:
        self.metric_builders: List[SimpleBigQueryViewBuilder] = []
        unified_query_select_clauses = []
        for metric in METRICS:
            view_builders = view_chain_for_metric(metric)
            unified_query_select_clauses.append(
                f"SELECT * FROM `{{project_id}}.{{calculation_dataset}}.{view_builders[-1].view_id}_materialized`"
            )
            self.metric_builders.extend(view_builders)

        self.unified_builder = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_DASHBOARD_DATASET,
            view_id="unified_corrections_metrics_by_month",
            view_query_template=" UNION ALL ".join(unified_query_select_clauses) + ";",
            description="Unified view of all calculated corrections metrics by month",
            calculation_dataset=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        )

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        return self.metric_builders + [self.unified_builder]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = CorrectionsMetricsByMonthBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
