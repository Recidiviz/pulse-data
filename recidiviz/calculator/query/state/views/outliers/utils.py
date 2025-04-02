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
from collections import defaultdict

from more_itertools import one

from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.observations.event_type import EventType
from recidiviz.observations.observation_type_utils import (
    materialized_view_address_for_observation,
)
from recidiviz.outliers.constants import TREATMENT_STARTS
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import MetricOutcome, OutliersMetricConfig
from recidiviz.utils.types import assert_type


def most_recent_staff_attrs_cte() -> str:
    return f"""
    -- Use the most recent session to get the staff's attributes from the most recent session
    SELECT *
    FROM `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` 
    WHERE state_code IN ({list_to_query_string(get_outliers_enabled_states_for_bigquery(), quoted=True)})
    QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, officer_id ORDER BY COALESCE(end_date_exclusive, "9999-01-01") DESC) = 1
"""


def officer_aggregated_metrics_plus_inclusion(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
) -> str:
    return f"""
    SELECT 
        m.*,
        -- flag to demarcate which officer metrics should be included in benchmark
        -- and outlier calculations
        include_in_outcomes
    FROM `{{project_id}}.aggregated_metrics.supervision_{unit_of_analysis_type.short_name}_aggregated_metrics_materialized` m
    INNER JOIN include_in_outcomes_cte
    USING (state_code, officer_id, end_date, period)
"""


def format_state_specific_person_events_filters(years_lookback: int = 2) -> str:
    """Builds a query that, for each state_code with Insights enabled, tells us every
    date where an event relevant to any configured metric happened (within the last X
    years, as defined by |years_lookback|..
    """
    state_specific_ctes = []

    state_specific_metric_configs_by_metric_name: dict[
        str, list[OutliersMetricConfig]
    ] = defaultdict(list)
    for state_code in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state_code)

        for metric in config.metrics:
            # TREATMENT_STARTS has custom logic and is handled in a separate cte
            if metric.name == TREATMENT_STARTS.name:
                continue

            state_specific_metric_configs_by_metric_name[metric.name].append(metric)

    for (
        metric_name,
        state_specific_metric_configs,
    ) in state_specific_metric_configs_by_metric_name.items():
        event_observation_type = one(
            {
                assert_type(c.event_observation_type, EventType)
                for c in state_specific_metric_configs
            }
        )

        metric_event_conditions_string = one(
            {c.metric_event_conditions_string for c in state_specific_metric_configs}
        )
        if not metric_event_conditions_string:
            raise ValueError(
                f"Expected a metric_event_conditions_string to be defined for metric "
                f"[{metric_name}]"
            )

        relevant_events_address = materialized_view_address_for_observation(
            event_observation_type
        )
        state_code_strs = sorted(
            assert_type(config.state_code, StateCode).value
            for config in state_specific_metric_configs
        )
        state_specific_ctes.append(
            f"""
SELECT 
    state_code,
    "{metric_name}" AS metric_id,
    event_date,
    person_id,
    CAST(NULL AS STRING) AS attributes
FROM `{{project_id}}.{relevant_events_address.to_str()}`
WHERE 
    state_code IN ({list_to_query_string(state_code_strs, quoted=True)})
    -- Limit the events lookback to minimize the size of the subqueries
    AND event_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL {str(years_lookback)} YEAR)
    AND ({metric_event_conditions_string})
"""
        )

    return "\nUNION ALL\n".join(state_specific_ctes)


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
        AND include_in_outcomes
    GROUP BY 1, 2, 3
"""
            )

    return "\n    UNION ALL\n".join(state_metric_specific_stmts)
