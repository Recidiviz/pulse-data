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
from typing import List

from recidiviz.aggregated_metrics.configuration.collections.zero_grants import (
    ZERO_GRANT_OPPORTUNITY_CONFIGURATIONS,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.calculator.query.state.views.outliers.supervision_metrics_helpers import (
    columns_for_unit_of_analysis,
    supervision_metric_query_template,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    most_recent_staff_attrs_cte,
    officer_aggregated_metrics_plus_inclusion,
)
from recidiviz.outliers.outliers_configs import get_outliers_backend_config
from recidiviz.outliers.types import OutliersMetricValueType, OutliersVitalsMetricConfig
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent

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

_INCLUDE_IN_OUTCOMES_METRICS_CTE = """
    WITH most_recent_include_in_outcomes AS (
        SELECT * from include_in_outcomes_cte
        -- We only need the most recent month's value for each officer
        WHERE end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)
        AND period="YEAR"
    )
    SELECT
        o.state_code,
        o.external_id as officer_id,
        IFNULL(end_date, DATE_TRUNC(CURRENT_DATE, MONTH)) as end_date,
        "YEAR" AS period,
        "ALL" AS category_type,
        "ALL" AS caseload_category,
        "BOOLEAN" AS value_type, 
        "include_in_outcomes" AS metric_id,
        CASE 
            WHEN i.include_in_outcomes THEN 1
            ELSE 0
        END AS metric_value,
    FROM
        `{project_id}.outliers_views.supervision_officers_materialized` o
    -- Left join so that we can set a value for every current officer, even if they weren't
    -- present in the CTE source tables.
    LEFT JOIN 
        most_recent_include_in_outcomes i
    ON 
      o.external_id = i.officer_id and o.state_code = i.state_code
"""


def _supervision_vitals_metric_query_template() -> str:
    """
    Gets the vitals metrics
    """
    vitals_states = []
    state_queries = []

    for state in get_outliers_enabled_states_for_bigquery():
        metrics_config: List[OutliersVitalsMetricConfig] = get_outliers_backend_config(
            state
        ).vitals_metrics

        if not metrics_config:  # Skip if no metrics
            continue

        vitals_states.append(state)
        VALID_METRICS = {"timely_contact", "timely_risk_assessment"}
        if invalid_metrics := {m.metric_id for m in metrics_config} - VALID_METRICS:
            raise ValueError(
                f"Invalid metrics found: {invalid_metrics}. Only {VALID_METRICS} are allowed."
            )

        metrics_list = ", ".join(f"'{metric.metric_id}'" for metric in metrics_config)

        query = f"""
            SELECT
                state_code,
                officer_id,
                end_date,
                metric_id,
                period,
                CASE 
                    WHEN metric_id = 'timely_contact' THEN ROUND((1 - IFNULL(SAFE_DIVIDE(avg_population_contact_overdue, avg_population_contact_required), 0)) * 100, 0)
                    WHEN metric_id = 'timely_risk_assessment' THEN ROUND((1 - IFNULL(SAFE_DIVIDE(avg_population_assessment_overdue, avg_population_assessment_required), 0)) * 100, 0)
                    ELSE NULL
                END AS metric_value,
                "RATE" AS value_type,
                "ALL" as caseload_category,
                "ALL" as category_type
            FROM day_agg_metrics
            CROSS JOIN UNNEST([{metrics_list}]) AS metric_id
            WHERE
                state_code = '{state}' AND
                end_date IN (
                    DATE_SUB((SELECT MAX(end_date) FROM day_agg_metrics), INTERVAL 1 DAY),
                    DATE_SUB((SELECT MAX(end_date) FROM day_agg_metrics), INTERVAL 30 DAY)
            )
        """
        state_queries.append(fix_indent(query, indent_level=4))

    unioned_state_queries = "\n    UNION ALL\n".join(state_queries)

    return f"""
    WITH day_agg_metrics AS (
        SELECT 
            state_code,
            officer_id,
            start_date,
            end_date,
            period,
            avg_population_contact_overdue,
            avg_population_contact_required,
            avg_population_assessment_overdue,
            avg_population_assessment_required
        FROM `{{project_id}}.vitals_report_views.supervision_officer_aggregated_metrics_materialized`
        WHERE state_code IN ({list_to_query_string(vitals_states, quoted=True)}) AND DATE_DIFF(end_date, start_date, DAY) = 1
    )\n{unioned_state_queries}
    """


_OFFICER_VITALS_METRICS = _supervision_vitals_metric_query_template()


def _zero_grants_metrics_cte() -> str:
    """Handles zero grants subqueries"""
    task_completions_subqueries = [
        f"""
        SELECT
            {list_to_query_string(columns_for_unit_of_analysis(MetricUnitOfAnalysisType.SUPERVISION_OFFICER))},
            "task_completions_{opp_config.opportunity_type}" AS metric_id,
            task_completions_{opp_config.task_completion_event.value} AS metric_value,
            "{OutliersMetricValueType.COUNT.value}" AS value_type,
            "ALL" as caseload_category,
            "ALL" as category_type
        FROM zg_agg_metrics
        WHERE state_code = '{opp_config.state_code.value}'
        -- For opportunity completions, we only need today's data.
        AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY)
    """
        for opp_config in ZERO_GRANT_OPPORTUNITY_CONFIGURATIONS
    ]

    prop_period_with_critical_caseload_subquery = f"""
        SELECT
            {list_to_query_string(columns_for_unit_of_analysis(MetricUnitOfAnalysisType.SUPERVISION_OFFICER))},
            "prop_period_with_critical_caseload" AS metric_id,
            prop_period_with_critical_caseload AS metric_value,
            "{OutliersMetricValueType.PROPORTION.value}" AS value_type,
            "ALL" as caseload_category,
            "ALL" as category_type
        FROM zg_agg_metrics
        -- This will be used in tandem with opportunity completions, so we only need today's data
        WHERE end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY)
    """

    zg_officer_metrics = "\nUNION ALL\n".join(
        task_completions_subqueries + [prop_period_with_critical_caseload_subquery]
    )
    return f"""
        WITH zg_agg_metrics as (
            SELECT m.* FROM `{{project_id}}.outliers_views.supervision_officer_aggregated_metrics_materialized` m
            -- only include metrics for current insights supervision officers
            INNER JOIN `{{project_id}}.outliers_views.supervision_officers_materialized` o
                ON m.state_code = o.state_code AND m.officer_id = o.external_id
            WHERE m.state_code IN ({list_to_query_string(get_outliers_enabled_states_for_bigquery(), quoted=True)})
        )
        {zg_officer_metrics}
    """


def _include_in_outcomes_cte() -> str:
    """
    Joins relevant officer aggregated metrics and attributes views to calculate the
    include_in_outcomes value for supervision officers across various periods.
    """
    state_specific_ctes = []

    attrs_join_statement = """
    INNER JOIN attrs
        ON attrs.state_code = o.state_code AND attrs.officer_id = o.external_id
"""
    for state_code in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state_code)
        state_specific_ctes.append(
            f"""
    SELECT
        m.state_code,
        m.officer_id,
        m.end_date,
        m.period,
        -- The include_in_outcomes column combines staff and metric exclusion conditions
        -- and is used to determine which officers' outcomes metrics rows should be 
        -- pulled into insights product views. The value for the most recent period
        -- will be stored as an officer metric row itself.
        {f"{config.supervision_staff_exclusions}" if config.supervision_staff_exclusions else "TRUE"} {config.supervision_officer_metric_exclusions if config.supervision_officer_metric_exclusions else ""} 
    AS include_in_outcomes
    FROM `{{project_id}}.aggregated_metrics.supervision_officer_aggregated_metrics_materialized` m
    -- Join on staff product view to ensure staff exclusions are applied
    INNER JOIN `{{project_id}}.outliers_views.supervision_officers_materialized` o
        ON m.state_code = o.state_code AND m.officer_id = o.external_id
    {attrs_join_statement if config.supervision_staff_exclusions else ""}
    WHERE m.state_code = "{state_code}"
"""
        )
    unioned_state_queries = "\n      UNION ALL\n".join(state_specific_ctes)
    return f"""
    WITH attrs AS (
        {most_recent_staff_attrs_cte()}
    )
    {unioned_state_queries}
"""


_QUERY_TEMPLATE = f"""
WITH 
include_in_outcomes_cte AS (
    {_include_in_outcomes_cte()}
),
supervision_officer_or_previous_if_transitional_aggregated_metrics_plus_inclusion AS (
  {officer_aggregated_metrics_plus_inclusion(MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL)}
),
supervision_officer_aggregated_metrics_plus_inclusion AS (
  {officer_aggregated_metrics_plus_inclusion(MetricUnitOfAnalysisType.SUPERVISION_OFFICER)}
),
supervision_officer_metrics AS (
    {supervision_metric_query_template(unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL, cte_source="supervision_officer_or_previous_if_transitional_aggregated_metrics_plus_inclusion")}
),
officer_caseload_categories AS (
    {_OFFICER_CASELOAD_CATEGORIES_CTE}
),
officer_vitals_metrics AS (
    {_OFFICER_VITALS_METRICS}
),
zero_grants_metrics AS (
    {_zero_grants_metrics_cte()}
),
officer_inclusion_metrics AS (
    {_INCLUDE_IN_OUTCOMES_METRICS_CTE}
)

SELECT 
    {{columns}},
FROM supervision_officer_metrics
INNER JOIN officer_caseload_categories
USING (state_code, officer_id, end_date, period)
FULL OUTER JOIN officer_vitals_metrics
USING (state_code, officer_id, end_date, metric_id, period, metric_value, value_type, caseload_category, category_type)
FULL OUTER JOIN zero_grants_metrics
USING (state_code, officer_id, end_date, metric_id, period, metric_value, value_type, caseload_category, category_type)
FULL OUTER JOIN officer_inclusion_metrics
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
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.build_and_print()
