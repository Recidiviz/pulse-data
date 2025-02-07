# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Functions to calculate the depth (transitions delta) metric for a given year
and set of disaggregating attributes."""

from typing import Optional

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import TRANSITIONS_DATASET_ID
from recidiviz.outcome_metrics.views.transitions_breadth_metric import (
    DISTINCT_COUNT_COLUMNS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent


def get_view_id_tag(
    metric_year: int = 2024, attribute_cols: Optional[list[str]] = None
) -> str:
    """Returns the view id tag indicating the metric year and additional
    grouping attribute columns"""
    attributes_tag = "__" + "_".join(attribute_cols) if attribute_cols else ""
    return f"_{metric_year}{attributes_tag}"


def get_transitions_baseline_metric_for_year(
    metric_year: int = 2024,
    attribute_cols: Optional[list[str]] = None,
) -> SimpleBigQueryViewBuilder:
    """Returns a view builder that represents the monthly transitions baseline for
    fully launched tools for the metric year. For old tools launched prior to the metric
    year, baseline is just the average monthly transitions from the previous year.
    For new tools launched during the metric year, baseline updates every month to
    include the average monthly transitions from the year prior to the full state
    launch."""
    attribute_cols_with_defaults = (attribute_cols if attribute_cols else []) + [
        "weight_factor",
        "delta_direction_factor",
    ]

    group_by_columns_query_fragment = "\n" + (
        fix_indent(",\n".join(attribute_cols_with_defaults), indent_level=8) + ",\n"
        if attribute_cols_with_defaults
        else ""
    )

    query_template = f"""
-- Count monthly transitions, deduped by person, event, and decarceral impact type,
-- for the metric year and the previous year.
WITH transitions_by_month AS (
    SELECT
        DATE_TRUNC(event_date, MONTH) AS transition_month,
        DATE_TRUNC(DATE(full_state_launch_date), MONTH) AS full_state_launch_month,{group_by_columns_query_fragment}
        COUNT(DISTINCT CONCAT({list_to_query_string(DISTINCT_COUNT_COLUMNS)})) AS transitions
    FROM
        `{{project_id}}.observations__person_event.impact_transition_materialized`
    WHERE
        EXTRACT(YEAR FROM event_date) IN ({metric_year - 1}, {metric_year})
    GROUP BY {group_by_columns_query_fragment}transition_month, DATE_TRUNC(DATE(full_state_launch_date), MONTH)
)
,
date_array AS (
    SELECT * FROM UNNEST(GENERATE_DATE_ARRAY("{metric_year}-01-01", "{metric_year}-12-31", INTERVAL 1 MONTH)) AS metric_month
)
,
-- Baseline for old launches is just the average monthly transitions count
-- from the previous year
baseline_agg_old_launches AS (
    SELECT
        metric_month,{group_by_columns_query_fragment}
        COALESCE(SUM(transitions)/12, 0) as monthly_baseline_old_tools,
    FROM
        date_array
    LEFT JOIN
        transitions_by_month
    ON
        transition_month BETWEEN "{metric_year - 1}-01-01" AND DATE_SUB("{metric_year}-01-01", INTERVAL 1 DAY)
        AND full_state_launch_month < "{metric_year}-01-01"
    GROUP BY {group_by_columns_query_fragment}metric_month
)
,
-- Baseline for new launches is the sum of monthly transitions for the year prior
-- to full state launch, among tools with a full state launch month falling before the 
-- metric month
baseline_agg_new_launches AS (
    SELECT
        metric_month,{group_by_columns_query_fragment}
        COALESCE(SUM(IF(metric_month >= full_state_launch_month, transitions, 0))/12, 0) as monthly_baseline_new_tools,
        COUNT(DISTINCT IF(full_state_launch_month = metric_month, full_state_launch_month, NULL)) AS num_new_launches,
    FROM
        date_array
    LEFT JOIN
        transitions_by_month
    ON
        transition_month BETWEEN DATE_SUB(full_state_launch_month, INTERVAL 1 YEAR) AND DATE_SUB(full_state_launch_month, INTERVAL 1 DAY)
        AND full_state_launch_month >= "{metric_year}-01-01"
    GROUP BY {group_by_columns_query_fragment}metric_month
)
SELECT 
    metric_month,
    {group_by_columns_query_fragment}
    IFNULL(num_new_launches, 0) AS num_new_launches,
    ROUND(IFNULL(monthly_baseline_old_tools, 0), 4) AS monthly_baseline_old_tools,
    ROUND(IFNULL(monthly_baseline_new_tools, 0), 4) AS monthly_baseline_new_tools,
    ROUND(IFNULL(monthly_baseline_old_tools, 0) + IFNULL(monthly_baseline_new_tools, 0), 4) AS total_baseline,
FROM 
    baseline_agg_old_launches
FULL OUTER JOIN
    baseline_agg_new_launches
USING
    (
{group_by_columns_query_fragment}metric_month
    )
WHERE
    CONCAT({list_to_query_string(attribute_cols_with_defaults)}) IS NOT NULL
ORDER BY {group_by_columns_query_fragment}metric_month
"""
    view_id = (
        f"transitions_baseline_metric{get_view_id_tag(metric_year, attribute_cols)}"
    )
    view_description = f"Baseline monthly transitions for year {metric_year}"

    return SimpleBigQueryViewBuilder(
        dataset_id=TRANSITIONS_DATASET_ID,
        view_query_template=query_template,
        view_id=view_id,
        description=view_description,
        should_materialize=True,
        clustering_fields=[],
    )


def get_transitions_depth_metric_for_year(
    metric_year: int = 2024,
    attribute_cols: Optional[list[str]] = None,
    breadth_metric_override: Optional[str] = None,
    baseline_override: Optional[str] = None,
) -> SimpleBigQueryViewBuilder:
    """Returns a view builder that represents the monthly transitions delta
    metric for the metric year, disaggregated by any attribute columns. Delta
    is calculated as the percent difference between the current month's transitions
    and the calculated baseline. If using custom queries for breadth or baseline metrics,
    use override fields (default is pre-materialized views)."""

    view_id = f"transitions_depth_metric{get_view_id_tag(metric_year, attribute_cols)}"
    view_description = f"Monthly transitions depth metric for {metric_year}"

    attribute_cols_with_defaults = (attribute_cols if attribute_cols else []) + [
        "weight_factor",
        "delta_direction_factor",
    ]

    attribute_cols_query_fragment = (
        list_to_query_string(attribute_cols_with_defaults) + ", "
    )
    breadth_metric_source = fix_indent(f"({breadth_metric_override})", indent_level=8)
    if not breadth_metric_override:
        breadth_metric_source = f"`{{project_id}}.transitions.transitions_breadth_metric{get_view_id_tag(metric_year, attribute_cols)}_materialized`"

    baseline_source = fix_indent(f"({baseline_override})", indent_level=8)
    if not baseline_override:
        baseline_source = f"`{{project_id}}.transitions.transitions_baseline_metric{get_view_id_tag(metric_year, attribute_cols)}_materialized`"

    partition_query_fragment = list_to_query_string(attribute_cols_with_defaults)

    query_template = f"""
SELECT
    metric_month,
    {fix_indent(attribute_cols_query_fragment, indent_level=4)}
    -- transitions from this month
    transitions AS transitions_month,
    -- transitions cumulative over all previous months in the year
    SUM(transitions) OVER (PARTITION BY {partition_query_fragment} ORDER BY metric_month) AS transitions_cumulative,
    -- monthly total baseline for this month
    total_baseline,
    -- prorated baseline accumulated over the all previous months in the year
    SUM(total_baseline) OVER (PARTITION BY {partition_query_fragment} ORDER BY metric_month) AS total_baseline_cumulative,
    -- transitions delta for this month
    ROUND((transitions - total_baseline)/total_baseline, 4) AS transitions_delta_month,
    -- cumulative delta percent, which divides cumulative delta by prorated baseline
    ROUND(
        SUM(transitions - total_baseline) 
            OVER (PARTITION BY {partition_query_fragment} ORDER BY metric_month) /
        SUM(total_baseline)
            OVER (PARTITION BY {partition_query_fragment} ORDER BY metric_month)
    , 4) AS transitions_delta_cumulative,
FROM
    {breadth_metric_source}
LEFT JOIN
    {baseline_source}
USING
    ({attribute_cols_query_fragment}metric_month)
"""
    return SimpleBigQueryViewBuilder(
        dataset_id=TRANSITIONS_DATASET_ID,
        view_query_template=query_template,
        view_id=view_id,
        description=view_description,
        should_materialize=True,
        clustering_fields=[],
    )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_transitions_baseline_metric_for_year(
            2024, ["product_transition_type"]
        ).build_and_print()
