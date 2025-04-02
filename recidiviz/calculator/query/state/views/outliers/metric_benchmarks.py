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
"""Metrics that are aggregated at the state or caseload level and used as the benchmarks/targets."""
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.supervision_metrics_helpers import (
    supervision_metric_query_template,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    get_highlight_percentile_value_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "metric_benchmarks"

_DESCRIPTION = """Metrics that are aggregated at the state or caseload level and used as the benchmarks/targets."""


_QUERY_TEMPLATE = f"""

WITH 
statewide_iqrs AS (
    SELECT
        state_code,
        end_date,
        metric_id,
        caseload_category,
        category_type,
        APPROX_QUANTILES(metric_value, 4)[OFFSET(3)] - APPROX_QUANTILES(metric_value, 4)[OFFSET(1)] AS iqr
    FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_metrics_materialized`
    WHERE value_type = 'RATE' AND period = 'YEAR'
    GROUP BY 1, 2, 3, 4, 5
)
, statewide_highlight_values AS (
    {get_highlight_percentile_value_query()}
)
-- TODO(#24119): Add highlight calculation by caseload type
, metrics_by_caseload_type AS (
    SELECT
        state_code,
        -- "ALL" indicates the metric is statewide.
        -- Pull statewide metrics from here instead of using the "ALL" category type in the
        -- insights caseload category metrics because the statewide metrics count clients who
        -- don't have an officer assignment, whereas the caseload category metrics only count
        -- clients that do.
        "ALL" AS caseload_category,
        "ALL" AS category_type,
        period,
        end_date,
        metric_id,
        metric_value,
        value_type
        FROM (
            {supervision_metric_query_template(unit_of_analysis_type=MetricUnitOfAnalysisType.STATE_CODE)}
        )
    
    UNION ALL
    
    SELECT * FROM (
        {supervision_metric_query_template(unit_of_analysis_type=MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY, dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET)}
    )
    WHERE category_type != "ALL"
)
, metric_benchmarks AS (
    SELECT 
        m.state_code,
        m.metric_id,
        m.period,
        m.end_date,
        caseload_category,
        -- TODO(#31634): Remove caseload_type
        caseload_category AS caseload_type,
        category_type,
        m.metric_value AS target,
        statewide_iqrs.iqr AS threshold,
        statewide_highlight_values.top_x_pct AS top_x_pct,
        statewide_highlight_values.top_x_pct_percentile_value
    FROM metrics_by_caseload_type m
    LEFT JOIN statewide_iqrs
        USING (state_code, metric_id, end_date, caseload_category, category_type)
    LEFT JOIN statewide_highlight_values
        USING (state_code, metric_id, end_date)
    WHERE
        m.value_type = 'RATE'
        -- It's possible for us to have a target for a category but not a threshold for it because
        -- of the way these metrics are produced. When calculating the metrics for each category, we
        -- calculate them based on the officer's caseload category as of each day of the calculation
        -- period, but when comparing the officers (at least for the SEX_OFFENSE_BINARY category
        -- type) we do it based on whether or not they had that category for the entire year. This
        -- means that, for example, if the earliest data we have for a SEX_OFFENSE caseload is from
        -- April 2023, then the period from 2023-03-01 to 2024-03-01 will have a target for that
        -- caseload type (because during the year prior there were officers with that type), but not
        -- a threshold for it (because nobody had the SEX_OFFENSE caseload type for the whole year-
        -- the March 2023 data is missing)
        AND statewide_iqrs.iqr IS NOT NULL
)

SELECT 
    {{columns}}
FROM 
    metric_benchmarks
"""

METRIC_BENCHMARKS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "metric_id",
        "period",
        "end_date",
        "caseload_category",
        "caseload_type",
        "category_type",
        "target",
        "threshold",
        "top_x_pct",
        "top_x_pct_percentile_value",
    ],
    outliers_views_dataset=dataset_config.OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        METRIC_BENCHMARKS_VIEW_BUILDER.build_and_print()
