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
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "metric_benchmarks"

_DESCRIPTION = """Metrics that are aggregated at the state or caseload level and used as the benchmarks/targets."""


_QUERY_TEMPLATE = """

WITH 
statewide_iqrs AS (
    SELECT
        state_code,
        end_date,
        metric_id,
        APPROX_QUANTILES(metric_value, 4)[OFFSET(3)] - APPROX_QUANTILES(metric_value, 4)[OFFSET(1)] AS iqr
    FROM `{project_id}.outliers_views.supervision_officer_metrics_materialized`
    WHERE value_type = 'RATE' AND period = 'YEAR'
    GROUP BY 1, 2, 3
)
-- TODO(#24119): Add iqr calculation by caseload type
, metric_benchmarks AS (
    SELECT 
        m.state_code,
        m.metric_id,
        m.period,
        m.end_date,
        -- Keep an entry where caseload type is NULL to indicate that the benchmark is statewide
        -- Use an empty string instead of NULL since SQL doesn't join on NULLs
        '' AS caseload_type,
        m.metric_value AS target,
        statewide_iqrs.iqr AS threshold
    FROM `{project_id}.outliers_views.supervision_state_metrics_materialized` m
    LEFT JOIN statewide_iqrs
        USING (state_code, metric_id, end_date)
    WHERE m.value_type = 'RATE'
-- TODO(#24119): Add metrics aggregated by caseload type
)

SELECT 
    {columns}
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
        "caseload_type",
        "target",
        "threshold",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        METRIC_BENCHMARKS_VIEW_BUILDER.build_and_print()
