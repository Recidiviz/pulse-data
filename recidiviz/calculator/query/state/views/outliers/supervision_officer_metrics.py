# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Aggregated metrics at the officer-level for supervision-related metrics"""
from typing import List, Literal, LiteralString

from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.calculator.query.state.views.outliers.supervision_metrics_helpers import (
    supervision_metric_query_template,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    format_state_specific_officer_aggregated_metric_filters,
)
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import OutliersVitalsMetricConfig
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officer_metrics"

_DESCRIPTION = (
    """Aggregated metrics at the officer-level for supervision-related metrics"""
)


def _get_caseload_category_criteria_for_comparison(
    category_type: InsightsCaseloadCategoryType,
) -> str:
    if category_type == InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY:
        # For this category type, we want to compare officers who had the SEX_OFFENSE category
        # for the entirety of the comparison period against other officers who had the SEX_OFFENSE
        # category for the entirety of the comparison period, and all others against each other.
        # To determine this, check that the amount of time they spent with the SEX_OFFENSE category
        # is the same as the amount of time they spent with the ALL category, that way if someone
        # only spent part of the year with a caseload at all, they still count as having the
        # SEX_OFFENSE type.
        # Note: another option would be to check that the not_sex_offense version = 0, but this
        # option felt more intuitive / avoids the double negative.
        return """IF(avg_num_supervision_officers_insights_sex_offense_binary_category_type_sex_offense
            = avg_num_supervision_officers_insights_all_category_type_all, "SEX_OFFENSE", "NOT_SEX_OFFENSE")
"""
    if category_type == InsightsCaseloadCategoryType.ALL:
        return '"ALL"'
    raise TypeError(
        f"Caseload categorization has not been configured for {category_type.name} category type."
    )


_OFFICER_CASELOAD_CATEGORIES_CTE = "\n    UNION ALL\n".join(
    [
        f"""
    SELECT
        state_code,
        officer_id,
        end_date,
        period,
        "{category_type.name}" AS category_type,
        {_get_caseload_category_criteria_for_comparison(category_type)} AS caseload_category,
    FROM
        `{{project_id}}.aggregated_metrics.supervision_officer_aggregated_metrics_materialized`
"""
        for category_type in InsightsCaseloadCategoryType
    ]
)


def _supervision_vitals_metric_query_template() -> str:
    """
    Gets the vitals metrics
    """
    state_queries = []

    for state in get_outliers_enabled_states_for_bigquery():
        metrics_config: List[OutliersVitalsMetricConfig] = get_outliers_backend_config(
            state
        ).vitals_metrics

        if not metrics_config:  # Skip if no metrics
            continue

        VALID_METRICS = {"timely_contact", "timely_risk_assessment"}
        if invalid_metrics := {m.metric_id for m in metrics_config} - VALID_METRICS:
            raise ValueError(
                f"Invalid metrics found: {invalid_metrics}. Only {VALID_METRICS} are allowed."
            )

        metrics_list = ", ".join(f"'{metric.metric_id}'" for metric in metrics_config)

        agg_metrics = "vitals_report_views.supervision_officer_day_aggregated_metrics_materialized"

        query = f"""
            SELECT
                state_code,
                officer_id,
                end_date,
                metric_id,
                period,
                CASE 
                    WHEN metric_id = 'timely_contact' THEN ROUND((1 - SAFE_DIVIDE(avg_population_contact_overdue, avg_population_contact_required)) * 100, 0)
                    WHEN metric_id = 'timely_risk_assessment' THEN ROUND((1 - SAFE_DIVIDE(avg_population_assessment_overdue, avg_population_assessment_required)) * 100, 0)
                    ELSE NULL
                END AS metric_value,
                "RATE" AS value_type,
                "ALL" as caseload_category,
                "ALL" as category_type
            FROM
                `{{project_id}}.{agg_metrics}`
            CROSS JOIN UNNEST([{metrics_list}]) AS metric_id
            WHERE
                state_code = '{state}' AND
                end_date IN (
                    DATE_SUB((SELECT MAX(end_date) FROM `{{project_id}}.{agg_metrics}`), INTERVAL 1 DAY),
                    DATE_SUB((SELECT MAX(end_date) FROM `{{project_id}}.{agg_metrics}`), INTERVAL 30 DAY)
            )
        """
        state_queries.append(query.strip())

    return "\nUNION ALL\n".join(state_queries)


_OFFICER_VITALS_METRICS: LiteralString | Literal[
    ""
] = _supervision_vitals_metric_query_template()


_QUERY_TEMPLATE = f"""
WITH 
filtered_supervision_officer_aggregated_metrics AS (
  {format_state_specific_officer_aggregated_metric_filters()}
),
supervision_officer_metrics AS (
    {supervision_metric_query_template(unit_of_analysis=METRIC_UNITS_OF_ANALYSIS_BY_TYPE[MetricUnitOfAnalysisType.SUPERVISION_OFFICER], cte_source="filtered_supervision_officer_aggregated_metrics")}
),
officer_caseload_categories AS (
    {_OFFICER_CASELOAD_CATEGORIES_CTE}
),
officer_vitals_metrics AS (
    {_OFFICER_VITALS_METRICS}
)

SELECT 
    {{columns}},
FROM supervision_officer_metrics
INNER JOIN officer_caseload_categories
USING (state_code, officer_id, end_date, period)
FULL OUTER JOIN officer_vitals_metrics
USING (state_code, officer_id, end_date, metric_id, period, metric_value, value_type, caseload_category, category_type)
"""

SUPERVISION_OFFICER_METRICS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "metric_id",
        "metric_value",
        "period",
        "end_date",
        "officer_id",
        "value_type",
        "category_type",
        "caseload_category",
        "include_in_outcomes",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.build_and_print()
