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

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
    PersonRecordType,
)
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import OutliersMetricValueType


def supervision_metric_query_template(
    unit_of_analysis: MetricUnitOfAnalysis,
    cte_source: Optional[str] = None,
    dataset_id: Optional[str] = AGGREGATED_METRICS_DATASET_ID,
) -> str:
    """
    Helper for querying supervision_<unit_of_analysis>_aggregated_metrics views
    """
    source_table = (
        f"`{{project_id}}.{dataset_id}.supervision_{unit_of_analysis.type.short_name}_aggregated_metrics_materialized`"
        if not cte_source
        else cte_source
    )
    subqueries = []
    for state_code in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state_code)

        subqueries.append(
            f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "avg_daily_population" AS metric_id,
    avg_daily_population AS metric_value,
    "{OutliersMetricValueType.AVERAGE.value}" AS value_type
FROM {source_table}
WHERE state_code = '{state_code}'
AND period = "YEAR"
-- Limit the events lookback to only the necessary periods to minimize the size of the subqueries
AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH)
        """
        )

        for metric in config.metrics:
            count_subquery = f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "{metric.name}" AS metric_id,
    {metric.name} AS metric_value,
    "{OutliersMetricValueType.COUNT.value}" AS value_type
FROM {source_table}
WHERE state_code = '{state_code}'
AND period = "YEAR"
-- Limit the events lookback to only the necessary periods to minimize the size of the subqueries
AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH)
"""

            rate_subquery = f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "{metric.name}" AS metric_id,
    {metric.name} / avg_daily_population AS metric_value,
    "{OutliersMetricValueType.RATE.value}" AS value_type
FROM {source_table}
WHERE state_code = '{state_code}'
AND period = "YEAR"
-- Limit the events lookback to only the necessary periods to minimize the size of the subqueries
AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH)
"""

            subqueries.extend([rate_subquery, count_subquery])
        # If we're building an officer query, append subqueries for officer task
        # completions (metrics that are unrelated to outlier outcomes)
        if unit_of_analysis.type == MetricUnitOfAnalysisType.SUPERVISION_OFFICER:
            for opp_config in [
                c
                for c in WORKFLOWS_OPPORTUNITY_CONFIGS
                if c.state_code.value == state_code
                and c.person_record_type == PersonRecordType.CLIENT
            ]:
                completion_count_subquery = f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "task_completions_{opp_config.opportunity_type}" AS metric_id,
    task_completions_{opp_config.task_completion_event.value} AS metric_value,
    "{OutliersMetricValueType.COUNT.value}" AS value_type
FROM {source_table}
WHERE state_code = '{state_code}'
AND period = "YEAR"
-- For opportunity completions, we only need the most recent month's data.
AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)
"""
                subqueries.append(completion_count_subquery)

            officer_active_prop_subquery = f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "prop_period_with_critical_caseload" AS metric_id,
    prop_period_with_critical_caseload AS metric_value,
    "{OutliersMetricValueType.PROPORTION.value}" AS value_type
FROM {source_table}
WHERE state_code = '{state_code}'
AND period = "YEAR"
-- This will be used in tandem with opportunity completions, so we only need the most recent month's data.
AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)
"""
            subqueries.append(officer_active_prop_subquery)

    query = "\nUNION ALL\n".join(subqueries)
    return query
