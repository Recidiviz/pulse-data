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

from typing import List, Optional

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.calculator.query.justice_counts.views import metric_calculator
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_STATE_CODE_AGGREGATION = metric_calculator.Aggregation(
    dimension=manual_upload.State, comprehensive=False
)

METRICS = [
    # Admissions Metrics
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS",
    ),
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[manual_upload.AdmissionType.NEW_COMMITMENT],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_NEW_COMMITMENTS",
    ),
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PAROLE,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PAROLE",
    ),
    metric_calculator.CalculatedMetric(
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
    metric_calculator.CalculatedMetric(
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
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionType.PROBATION,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_PROBATION",
    ),
    metric_calculator.CalculatedMetric(
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
    metric_calculator.CalculatedMetric(
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
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionViolationType.NEW_CRIME,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_ALL_SUPERVISION_NEW_CRIME",
    ),
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.ADMISSIONS,
        filtered_dimensions=[
            manual_upload.AdmissionType.FROM_SUPERVISION,
            manual_upload.SupervisionViolationType.TECHNICAL,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="ADMISSIONS_FROM_ALL_SUPERVISION_TECHNICAL",
    ),
    # Supervision Starts
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.SUPERVISION_STARTS,
        filtered_dimensions=[manual_upload.SupervisionType.PROBATION],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="SUPERVISION_STARTS_PROBATION",
    ),
    # Population Metrics
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.POPULATION,
        filtered_dimensions=[
            manual_upload.PopulationType.SUPERVISION,
            manual_upload.SupervisionType.PAROLE,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="POPULATION_PAROLE",
    ),
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.POPULATION,
        filtered_dimensions=[manual_upload.PopulationType.PRISON],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="POPULATION_PRISON",
    ),
    metric_calculator.CalculatedMetric(
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
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.RELEASES,
        filtered_dimensions=[],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="RELEASES",
    ),
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.RELEASES,
        filtered_dimensions=[manual_upload.ReleaseType.TO_SUPERVISION],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="RELEASES_TO_ALL_SUPERVISION",
    ),
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.RELEASES,
        filtered_dimensions=[
            manual_upload.ReleaseType.TO_SUPERVISION,
            manual_upload.SupervisionType.PAROLE,
        ],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="RELEASES_TO_PAROLE",
    ),
    metric_calculator.CalculatedMetric(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.RELEASES,
        filtered_dimensions=[manual_upload.ReleaseType.COMPLETED],
        aggregated_dimensions={"state_code": _STATE_CODE_AGGREGATION},
        output_name="RELEASES_COMPLETED",
    ),
]

ANNUAL_MONTH_BOUNDARIES_VIEW_TEMPLATE = """
SELECT
  dimensions_string as state_code,
  priority,
  max(date_partition) as date_partition
FROM (
  SELECT
    *,
    -- Calculate date partition as the first day of the first month not covered by the time window.
    DATE_ADD(DATE_TRUNC(DATE_SUB(instance.time_window_end, INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH) AS date_partition,
    -- Bucket instances into priorities, such that the longest windows determine which month to partition on.
    CASE
        WHEN DATE_DIFF(time_window_end, time_window_start, DAY) > 300 THEN 1
        WHEN DATE_DIFF(time_window_end, time_window_start, DAY) > 80 THEN 2
        WHEN DATE_DIFF(time_window_end, time_window_start, DAY) > 25 THEN 3
        ELSE 4
    END AS priority
  FROM
    `{project_id}.{input_dataset}.{input_table}` input
  JOIN
    `{project_id}.{base_dataset}.report_table_instance_materialized` instance
  ON
    input.instance_id = instance.id ) input_with_date_partition
GROUP BY
  dimensions_string, priority
"""


class AnnualMonthBoundariesViewBuilder(SimpleBigQueryViewBuilder):
    """Calculates the most recent month boundary for each state for the given metric."""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_to_calculate: metric_calculator.CalculatedMetric,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_to_calculate.view_prefix}_annual_month",
            view_query_template=ANNUAL_MONTH_BOUNDARIES_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_to_calculate.output_name} annual months",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
        )


PICK_ANNUAL_MONTH_BOUNDARIES_QUERY_TEMPLATE = """
SELECT
  state_code,
  date_partition
FROM (
  SELECT
    state_code,
    priority,
    date_partition,
    MIN(priority) OVER (PARTITION BY state_code) as min_priority,
    -- Label by frequency within a state, priority partition
    ROW_NUMBER() OVER (PARTITION BY state_code, priority ORDER BY COUNT(*) DESC,
      date_partition DESC) AS rn
  FROM
    `{project_id}.{input_dataset}.{input_table}`
  GROUP BY state_code, priority, date_partition) partition_with_rn
WHERE
  priority = min_priority
  AND rn = 1
"""


class PickAnnualMonthBoundaryViewBuilder(SimpleBigQueryViewBuilder):
    """Picks a single annual month boundary for each state that is compatible with the
    most metrics."""

    def __init__(
        self,
        *,
        dataset_id: str,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id="pick_annual_month",
            view_query_template=PICK_ANNUAL_MONTH_BOUNDARIES_QUERY_TEMPLATE,
            should_materialize=True,
            # Query Format Arguments
            description="picks annual month for each state based on all metrics",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
        )


OUTPUT_VIEW_TEMPLATE = """
SELECT {aggregated_dimension_columns},
       '{metric_output_name}' as metric,
       EXTRACT(YEAR from DATE_SUB(date_partition, INTERVAL 1 DAY)) as year,
       EXTRACT(MONTH from DATE_SUB(date_partition, INTERVAL 1 DAY)) as month,
       DATE_SUB(time_window_end, INTERVAL 1 DAY) as date_reported,
       source.name as source_name,
       report.url as source_url,
       report_type as report_name,
       report.publish_date as date_published,
       measurement_type as measurement_type,
       ARRAY(
         SELECT DISTINCT(collapsed_dimension_value)
         FROM UNNEST(collapsed_dimension_values) as collapsed_dimension_value
         ORDER BY collapsed_dimension_value
       ) as raw_source_categories,
       value as value,
       -- If there is no row at least a year back to compare to, the following columns will be NULL.
       EXTRACT(YEAR from DATE_SUB(compare_date_partition, INTERVAL 1 DAY)) as compared_to_year,
       EXTRACT(MONTH from DATE_SUB(compare_date_partition, INTERVAL 1 DAY)) as compared_to_month,
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
        metric_to_calculate: metric_calculator.CalculatedMetric,
        input_view: BigQueryViewBuilder,
        view_id_suffix: Optional[str] = None,
    ):
        aggregated_dimension_columns = ", ".join(
            metric_to_calculate.aggregated_dimensions
        )

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_to_calculate.view_prefix}_output{view_id_suffix}",
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
    metric: metric_calculator.CalculatedMetric,
    input_view: SimpleBigQueryViewBuilder,
    metric_name_suffix: str,
) -> List[SimpleBigQueryViewBuilder]:
    comparison_view_builder = metric_calculator.CompareToPriorYearViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        metric_name=metric.output_name + metric_name_suffix,
        input_view=input_view,
    )
    dimensions_to_columns_view_builder = (
        metric_calculator.DimensionsToColumnsViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
            metric_name=metric.output_name + metric_name_suffix,
            aggregations=metric.aggregated_dimensions,
            input_view=comparison_view_builder,
        )
    )
    output_view_builder = CorrectionsOutputViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        metric_to_calculate=metric,
        input_view=dimensions_to_columns_view_builder,
        view_id_suffix=metric_name_suffix,
    )

    return [
        comparison_view_builder,
        dimensions_to_columns_view_builder,
        output_view_builder,
    ]


class CorrectionsMetricsBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """Creates the chain of views required to calculate corrections metrics."""

    def __init__(self) -> None:
        self.metric_builders: List[SimpleBigQueryViewBuilder] = []
        self.monthly_builders: List[SimpleBigQueryViewBuilder] = []
        self.annual_builders: List[SimpleBigQueryViewBuilder] = []

        view_chains: List[List[SimpleBigQueryViewBuilder]] = []
        for metric in METRICS:
            view_chain = metric_calculator.fetch_metric_view_chain(
                dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
                metric_to_calculate=metric,
            )
            view_chains.append(view_chain)
            self.metric_builders.extend(view_chain)

        self.build_views_to_pick_annual_month_boundary(view_chains)

        monthly_unified_query_select_clauses = []
        for metric, initial_chain in zip(METRICS, view_chains):
            monthly_view_chain = metric_calculator.time_aggregation_view_chain(
                input_view=initial_chain[-1],
                dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
                metric_to_calculate=metric,
                time_aggregation=metric_calculator.TimeAggregation.MONTHLY,
            )
            self.monthly_builders.extend(monthly_view_chain)

            output_view_chain = view_chain_for_metric(
                metric, monthly_view_chain[-1], "_monthly"
            )
            self.monthly_builders.extend(output_view_chain)

            monthly_unified_query_select_clauses.append(
                f"SELECT * FROM `{{project_id}}.{{calculation_dataset}}.{output_view_chain[-1].view_id}_materialized`"
            )

        self.monthly_unified_builder = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_DASHBOARD_DATASET,
            view_id="unified_corrections_metrics_monthly",
            view_query_template=" UNION ALL ".join(
                monthly_unified_query_select_clauses
            ),
            description="Unified view of all calculated corrections metrics by month",
            calculation_dataset=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        )

        annual_unified_query_select_clauses = []
        for metric, initial_chain in zip(METRICS, view_chains):
            annual_view_chain = metric_calculator.time_aggregation_view_chain(
                input_view=initial_chain[-1],
                dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
                metric_to_calculate=metric,
                time_aggregation=metric_calculator.TimeAggregation.ANNUAL,
                date_partition_table=self.pick_annual_month_builder.table_for_query,
            )
            self.annual_builders.extend(annual_view_chain)

            output_view_chain = view_chain_for_metric(
                metric, annual_view_chain[-1], "_annual"
            )
            self.annual_builders.extend(output_view_chain)

            annual_unified_query_select_clauses.append(
                f"SELECT * FROM `{{project_id}}.{{calculation_dataset}}.{output_view_chain[-1].view_id}_materialized`"
            )

        self.annual_unified_builder = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_DASHBOARD_DATASET,
            view_id="unified_corrections_metrics_annual",
            view_query_template=" UNION ALL ".join(annual_unified_query_select_clauses),
            description="Unified view of all calculated corrections metrics by month",
            calculation_dataset=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        )

    def build_views_to_pick_annual_month_boundary(
        self, metric_view_chains: List[List[SimpleBigQueryViewBuilder]]
    ) -> None:
        self.annual_month_builders: List[SimpleBigQueryViewBuilder] = []
        annual_month_select_clauses = []
        for metric, initial_chain in zip(METRICS, metric_view_chains):
            annual_month_builder = AnnualMonthBoundariesViewBuilder(
                dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
                metric_to_calculate=metric,
                input_view=initial_chain[-1],
            )
            annual_month_select_clauses.append(
                f"SELECT * FROM `{{project_id}}.{{input_dataset}}.{annual_month_builder.table_for_query.table_id}`"
            )
            self.annual_month_builders.append(annual_month_builder)
        self.annual_month_builder = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
            view_id="annual_months",
            view_query_template=" UNION ALL ".join(annual_month_select_clauses),
            description="",
            input_dataset=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
        )
        self.pick_annual_month_builder = PickAnnualMonthBoundaryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
            input_view=self.annual_month_builder,
        )

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        return (
            self.metric_builders
            + self.annual_month_builders
            + [
                self.annual_month_builder,
                self.pick_annual_month_builder,
            ]
            + self.monthly_builders
            + self.annual_builders
            + [
                self.monthly_unified_builder,
                self.annual_unified_builder,
            ]
        )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = CorrectionsMetricsBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
