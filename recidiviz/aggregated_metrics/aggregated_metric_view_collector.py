# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Returns all aggregated metric view builders for specified populations and aggregation levels"""
from typing import Dict, List

from recidiviz.aggregated_metrics.aggregated_metrics import (
    generate_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.assignment_event_aggregated_metrics import (
    generate_assignment_event_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.assignment_span_aggregated_metrics import (
    generate_assignment_span_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.metrics_assignment_sessions import (
    generate_metric_assignment_sessions_view_builder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_AGE,
    AVG_DAILY_POPULATION,
    AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT,
    AVG_DAILY_POPULATION_FEMALE,
    AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
    AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
    AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
    AVG_DAILY_POPULATION_NONWHITE,
    AVG_DAILY_POPULATION_PAROLE_BOARD_HOLD,
    AVG_DAILY_POPULATION_SHOCK_INCARCERATION,
    AVG_DAILY_POPULATION_TREATMENT_IN_PRISON,
    AVG_LSIR_SCORE,
    DAYS_INCARCERATED_365,
    DAYS_SINCE_MOST_RECENT_LSIR,
    DAYS_TO_FIRST_SUPERVISION_START_365,
    SUPERVISION_STARTS,
)
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    METRIC_AGGREGATION_LEVELS_BY_TYPE,
    MetricAggregationLevelType,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.period_event_aggregated_metrics import (
    generate_period_event_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.period_span_aggregated_metrics import (
    generate_period_span_aggregated_metrics_view_builder,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder

METRICS_BY_POPULATION_TYPE: Dict[MetricPopulationType, List[AggregatedMetric]] = {
    MetricPopulationType.INCARCERATION: [
        # Average daily population
        AVG_DAILY_POPULATION,
        # Average daily population, person demographics
        AVG_AGE,
        AVG_DAILY_POPULATION_FEMALE,
        AVG_DAILY_POPULATION_NONWHITE,
        # Average daily population, compartment level 2
        AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT,
        AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
        AVG_DAILY_POPULATION_PAROLE_BOARD_HOLD,
        AVG_DAILY_POPULATION_SHOCK_INCARCERATION,
        AVG_DAILY_POPULATION_TREATMENT_IN_PRISON,
        # Risk score
        AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
        AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
        AVG_LSIR_SCORE,
        DAYS_SINCE_MOST_RECENT_LSIR,
        # Events
        SUPERVISION_STARTS,
        # Assignment window metrics
        DAYS_INCARCERATED_365,
        DAYS_TO_FIRST_SUPERVISION_START_365,
    ],
    MetricPopulationType.SUPERVISION: [],
}

LEVELS_BY_POPULATION_TYPE: Dict[
    MetricPopulationType, List[MetricAggregationLevelType]
] = {
    MetricPopulationType.INCARCERATION: [
        MetricAggregationLevelType.FACILITY,
        MetricAggregationLevelType.STATE_CODE,
    ],
    MetricPopulationType.SUPERVISION: [],
}


def collect_aggregated_metrics_view_builders() -> List[SimpleBigQueryViewBuilder]:
    """
    Collects all aggregated metrics view builders at all available aggregation levels and populations
    """
    view_builders = []
    for population_type in MetricPopulationType:
        population = METRIC_POPULATIONS_BY_TYPE[population_type]
        all_metrics = METRICS_BY_POPULATION_TYPE[population_type]
        if not all_metrics:
            continue
        for level_type in LEVELS_BY_POPULATION_TYPE[population_type]:
            level = METRIC_AGGREGATION_LEVELS_BY_TYPE[level_type]
            # Build assignment table
            view_builders.append(
                generate_metric_assignment_sessions_view_builder(
                    aggregation_level=level,
                    population=population,
                )
            )

            # Build metric builder views by type
            view_builders.append(
                generate_period_span_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, PeriodSpanAggregatedMetric)
                    ],
                )
            )
            view_builders.append(
                generate_period_event_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, PeriodEventAggregatedMetric)
                    ],
                )
            )
            view_builders.append(
                generate_assignment_span_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, AssignmentSpanAggregatedMetric)
                    ],
                )
            )
            view_builders.append(
                generate_assignment_event_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, AssignmentEventAggregatedMetric)
                    ],
                )
            )

            # Build joined view of all metrics
            view_builders.append(
                generate_aggregated_metrics_view_builder(
                    aggregation_level=level, population=population
                )
            )
    return view_builders
