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
"""Helpers for building Outliers views"""
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.outliers.constants import TREATMENT_STARTS
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import MetricOutcome


def format_state_specific_officer_aggregated_metric_filters() -> str:
    state_specific_ctes = []

    for state_code in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state_code)
        state_specific_ctes.append(
            f"""
    SELECT 
        m.*
    FROM `{{project_id}}.aggregated_metrics.supervision_officer_aggregated_metrics_materialized` m
    -- Join on staff product view to ensure staff exclusions are applied
    INNER JOIN `{{project_id}}.outliers_views.supervision_officers_materialized` o
        ON m.state_code = o.state_code AND m.officer_id = o.external_id
    WHERE 
        m.state_code = '{state_code}' {config.supervision_officer_metric_exclusions if config.supervision_officer_metric_exclusions else ""}
        -- currently, the Outliers product only references metrics for 12-month periods
        AND m.period = 'YEAR'
"""
        )

    return "\n      UNION ALL\n".join(state_specific_ctes)


def format_state_specific_person_events_filters(years_lookback: int = 2) -> str:
    state_specific_ctes = []

    for state_code in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state_code)
        for metric in config.metrics:
            state_specific_ctes.append(
                f"""
    SELECT 
        state_code,
        "{metric.name}" AS metric_id,
        event_date,
        person_id,
        CAST(NULL AS STRING) AS attributes
    -- TODO(#29291): Refactor so we query only from the observation-specific views for 
    --   this metric
    FROM `{{project_id}}.observations__person_event.all_person_events_materialized`
    WHERE 
        state_code = '{state_code}' 
        -- Limit the events lookback to minimize the size of the subqueries
        AND event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {str(years_lookback)} YEAR)
        -- TREATMENT_STARTS has custom logic and is handled in a separate cte
        AND '{metric.name}' != '{TREATMENT_STARTS.name}'
        {f"AND ({metric.metric_event_conditions_string})" if metric.metric_event_conditions_string else ""}
"""
            )

    return "\n    UNION ALL\n".join(state_specific_ctes)


def get_highlight_percentile_value_query() -> str:
    """
    Returns unioned SELECT statements where each subquery calculates the
    percentile value that should be used to determine if an individual metric rate
    is in the top_x_pct for a given state, metric, and period end date.
    """
    state_metric_specific_stmts = []

    for state_code in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state_code)
        for metric in config.metrics:
            if metric.top_x_pct is None:
                continue

            if metric.outcome_type == MetricOutcome.FAVORABLE:
                # For favorable metrics, where performing above the target is positive, highlighting the top 10% means finding officers with rates above the 90th percentile.
                percentile = 100 - metric.top_x_pct
            else:
                # For adverse metrics, performing below the target is positive. Thus, calculating top 10% here means finding officers with rates below the 10th percentile value.
                percentile = metric.top_x_pct

            state_metric_specific_stmts.append(
                f"""
    SELECT
        state_code,
        end_date,
        metric_id,
        {metric.top_x_pct if metric.top_x_pct else 'NULL'} AS top_x_pct,
        APPROX_QUANTILES(metric_value, 100)[OFFSET({percentile})] AS top_x_pct_percentile_value
    FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_metrics_materialized`
    WHERE
        value_type = 'RATE'
        AND period = 'YEAR'
        AND metric_id = '{metric.name}'
        AND state_code = '{state_code}'
        AND category_type = 'ALL' -- highlights are done statewide for now
    GROUP BY 1, 2, 3
"""
            )

    return "\n    UNION ALL\n".join(state_metric_specific_stmts)
