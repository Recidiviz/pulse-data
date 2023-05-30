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
"""Creates view builders calculating miscellaneous metrics for specific aggregation levels and populations"""
from typing import Dict, List, Optional, Tuple

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.models.aggregated_metric import MiscAggregatedMetric
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_ASSIGNMENTS_OFFICER,
    AVG_CRITICAL_CASELOAD_SIZE,
    AVG_CRITICAL_CASELOAD_SIZE_OFFICER,
    AVG_DAILY_CASELOAD_OFFICER,
    PROP_PERIOD_WITH_CRITICAL_CASELOAD,
    SUPERVISION_DISTRICT,
    SUPERVISION_DISTRICT_INFERRED,
    SUPERVISION_OFFICE,
    SUPERVISION_OFFICE_INFERRED,
    SUPERVISION_UNIT,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    join_on_columns_fragment,
    nonnull_current_date_clause,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulation,
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)

# List of [MetricPopulationType, MetricUnitOfAnalysisType] tuples that are supported by
# `generate_misc_aggregated_metrics_view_builder` function.
_MISC_METRICS_SUPPORTED_POPULATIONS_AGGREGATION_LEVELS: List[
    Tuple[MetricPopulationType, MetricUnitOfAnalysisType]
] = [
    (MetricPopulationType.SUPERVISION, MetricUnitOfAnalysisType.SUPERVISION_OFFICER),
    (MetricPopulationType.SUPERVISION, MetricUnitOfAnalysisType.SUPERVISION_UNIT),
    (MetricPopulationType.SUPERVISION, MetricUnitOfAnalysisType.SUPERVISION_OFFICE),
    (MetricPopulationType.SUPERVISION, MetricUnitOfAnalysisType.SUPERVISION_DISTRICT),
    (MetricPopulationType.SUPERVISION, MetricUnitOfAnalysisType.STATE_CODE),
]

_MIN_OFFICER_CASELOAD_SIZE = 10


def _query_template_and_format_args(
    aggregation_level: MetricUnitOfAnalysis,
    population: MetricPopulation,
) -> Tuple[str, Dict[str, str]]:
    """Returns the appropriate query template (and associated dataset keyword args for
    that template) for the provided population and aggregation level.
    """
    if population.population_type == MetricPopulationType.SUPERVISION:
        if aggregation_level.level_type == MetricUnitOfAnalysisType.SUPERVISION_OFFICER:
            group_by_range = range(1, len(aggregation_level.primary_key_columns) + 9)
            group_by_range_str = ", ".join(list(map(str, group_by_range)))
            cte = f"""
    SELECT
        {aggregation_level.get_primary_key_columns_query_string(prefix="b")},
        period,
        population_start_date AS start_date,
        population_end_date AS end_date,
        c.district AS {SUPERVISION_DISTRICT_INFERRED.name},
        c.office AS {SUPERVISION_OFFICE_INFERRED.name},
        r.supervision_district AS {SUPERVISION_DISTRICT.name},
        r.supervision_office AS {SUPERVISION_OFFICE.name},
        r.supervision_unit AS {SUPERVISION_UNIT.name},
        -- Proportion of the analysis period where officer has a valid caseload size
        SUM(
            IF(
                b.caseload_count >= {_MIN_OFFICER_CASELOAD_SIZE},
                DATE_DIFF(
                    LEAST(a.population_end_date, {nonnull_current_date_clause("b.end_date")}),
                    GREATEST(a.population_start_date, b.start_date),
                    DAY
                ),
                0
            )
        ) / DATE_DIFF(population_end_date, population_start_date, DAY) AS {PROP_PERIOD_WITH_CRITICAL_CASELOAD.name},
    
        -- Average caseload size across all days in the analysis period where officer has a valid caseload
        SAFE_DIVIDE(
            SUM(
                IF(
                    b.caseload_count >= {_MIN_OFFICER_CASELOAD_SIZE},
                    DATE_DIFF(
                        LEAST(a.population_end_date, {nonnull_current_date_clause("b.end_date")}),
                        GREATEST(a.population_start_date, b.start_date),
                        DAY
                    ) * b.caseload_count,
                    0
                )
            )
            ,
            SUM(
                IF(
                    b.caseload_count >= {_MIN_OFFICER_CASELOAD_SIZE},
                    DATE_DIFF(
                        LEAST(a.population_end_date, {nonnull_current_date_clause("b.end_date")}),
                        GREATEST(a.population_start_date, b.start_date),
                        DAY
                    ),
                    0
                )
            )
        ) AS {AVG_CRITICAL_CASELOAD_SIZE.name},
    FROM
        `{{project_id}}.{{aggregated_metrics_dataset}}.metric_time_periods_materialized` a
    INNER JOIN
        `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_officer_caseload_count_spans_materialized` b
    ON
        b.start_date < a.population_end_date
        AND {nonnull_end_date_clause("b.end_date")} > a.population_start_date
    LEFT JOIN (
        SELECT 
            *, date AS population_start_date
        FROM
            `{{project_id}}.{{analyst_views_dataset}}.supervision_officer_primary_office_materialized`
        ) c
    USING 
        ({aggregation_level.get_primary_key_columns_query_string()}, population_start_date)
    LEFT JOIN 
        `{{project_id}}.{{sessions_dataset}}.supervision_officer_attribute_sessions_materialized` r
    ON
        {join_on_columns_fragment(columns=aggregation_level.primary_key_columns, table1="b", table2="r")}
        AND population_end_date BETWEEN r.start_date AND {nonnull_end_date_exclusive_clause("r.end_date_exclusive")}
    GROUP BY {group_by_range_str}
"""
            return cte, {
                "aggregated_metrics_dataset": AGGREGATED_METRICS_DATASET_ID,
                "analyst_views_dataset": ANALYST_VIEWS_DATASET,
                "sessions_dataset": SESSIONS_DATASET,
            }
        if aggregation_level.level_type in [
            MetricUnitOfAnalysisType.SUPERVISION_UNIT,
            MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
            MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
            MetricUnitOfAnalysisType.STATE_CODE,
        ]:
            cte = f"""
    SELECT
        {aggregation_level.get_primary_key_columns_query_string()},
        period,
        start_date,
        end_date,
        AVG(avg_daily_population) AS {AVG_DAILY_CASELOAD_OFFICER.name},
        AVG(avg_critical_caseload_size) AS {AVG_CRITICAL_CASELOAD_SIZE_OFFICER.name},
        AVG(assignments) AS {AVG_ASSIGNMENTS_OFFICER.name},
    FROM (
        SELECT 
            *, 
            {SUPERVISION_DISTRICT_INFERRED.name} AS district, 
            {SUPERVISION_OFFICE_INFERRED.name} AS office,
            {SUPERVISION_UNIT.name} AS unit,
        FROM
            `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_officer_aggregated_metrics_materialized`
    )
    GROUP BY
        {aggregation_level.get_primary_key_columns_query_string()},
        period, start_date, end_date       
"""
            return cte, {"aggregated_metrics_dataset": AGGREGATED_METRICS_DATASET_ID}

    raise ValueError(
        f"Unexpected population_type [{population.population_type}] and "
        f"aggregation_level type [{aggregation_level.level_type}] when generating misc "
        f"metrics."
    )


def generate_misc_aggregated_metrics_view_builder(
    aggregation_level: MetricUnitOfAnalysis,
    population: MetricPopulation,
    metrics: List[MiscAggregatedMetric],
) -> Optional[SimpleBigQueryViewBuilder]:
    """
    Returns a SimpleBigQueryViewBuilder that calculates miscellaneous metrics for the specified
    aggregation level and population, if one exists.
    """
    if (
        population.population_type,
        aggregation_level.level_type,
    ) not in _MISC_METRICS_SUPPORTED_POPULATIONS_AGGREGATION_LEVELS:
        return None

    cte, dataset_kwargs = _query_template_and_format_args(aggregation_level, population)
    metrics_str = ",\n    ".join([metric.name for metric in metrics])
    query_template = f"""
WITH misc_metrics_cte AS (
    {cte}
)
SELECT
    {aggregation_level.get_primary_key_columns_query_string()},
    start_date,
    end_date,
    period,
    {metrics_str}
FROM
    misc_metrics_cte
"""

    view_id = f"{population.population_name_short}_{aggregation_level.level_name_short}_misc_aggregated_metrics"
    view_description = f"""
    Metrics for the {population.population_name_short} population calculated using 
    ad-hoc logic, disaggregated by {aggregation_level.level_name_short}.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        should_materialize=False,
        clustering_fields=aggregation_level.primary_key_columns,
        # We set these values so that mypy knows they are not in the dataset_kwargs
        materialized_address_override=None,
        should_deploy_predicate=None,
        projects_to_deploy=None,
        **dataset_kwargs,
    )
