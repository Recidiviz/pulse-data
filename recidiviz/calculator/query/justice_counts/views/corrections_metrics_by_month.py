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


class CorrectionsMetricsByMonthBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    def __init__(self) -> None:
        self.metric_builders: List[SimpleBigQueryViewBuilder] = []
        unified_query_select_clauses = []
        for metric in METRICS:
            view_builder = metric_by_month.CalculatedMetricByMonthViewBuilder(
                dataset_id=dataset_config.JUSTICE_COUNTS_CORRECTIONS_DATASET,
                metric_to_calculate=metric,
            )
            unified_query_select_clauses.append(
                f"SELECT * FROM `{{project_id}}.{{calculation_dataset}}.{view_builder.view_id}_materialized`"
            )
            self.metric_builders.append(view_builder)

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
