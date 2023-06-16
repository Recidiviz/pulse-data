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
"""Helpers for building supervision metrics views """
from typing import Optional

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)
from recidiviz.outliers.outliers_configs import OUTLIERS_CONFIGS_BY_STATE


def supervision_metric_query_template(
    unit_of_analysis: MetricUnitOfAnalysis, cte_source: Optional[str] = None
) -> str:
    """
    Helper for querying supervision_<unit_of_analysis>_aggregated_metrics views
    """
    source_table = (
        f"`{{project_id}}.{{aggregated_metrics_dataset}}.supervision_{unit_of_analysis.level_name_short}_aggregated_metrics_materialized`"
        if not cte_source
        else cte_source
    )

    subqueries = []
    for state_code, config in OUTLIERS_CONFIGS_BY_STATE.items():
        subqueries.append(
            f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "avg_daily_population" AS metric_id,
    avg_daily_population AS metric_value,
FROM {source_table}
WHERE state_code = '{state_code.value}'
        """
        )

        for metric in config.metrics:
            subqueries.append(
                f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "{metric.name}" AS metric_id,
    {metric.name} AS metric_value,
FROM {source_table}
WHERE state_code = '{state_code.value}'
"""
            )

    query = "\nUNION ALL\n".join(subqueries)
    return query
