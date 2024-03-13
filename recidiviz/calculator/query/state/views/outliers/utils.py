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
from recidiviz.outliers.outliers_configs import get_outliers_backend_config


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
    FROM `{{project_id}}.analyst_data.person_events_materialized`
    WHERE 
        state_code = '{state_code}' 
        -- Limit the events lookback to minimize the size of the subqueries
        AND event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {str(years_lookback)} YEAR)
        {f"AND ({metric.metric_event_conditions_string})" if metric.metric_event_conditions_string else ""}
"""
            )

    return "\n    UNION ALL\n".join(state_specific_ctes)
