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
"""A script for building and writing a set of LookML views that support custom breadth and depth metrics
in Looker.

Run the following to write views to the specified directory DIR:
python -m recidiviz.tools.looker.top_level_generators.breadth_and_depth_lookml_generator [--looker-repo-root [DIR]]

"""

import argparse

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldDatatype,
    LookMLFieldParameter,
    MeasureLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldType
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.tools.looker.aggregated_metrics.custom_metrics_lookml_utils import (
    liquid_wrap_json_field,
)
from recidiviz.tools.looker.script_helpers import (
    get_generated_views_path,
    parse_and_validate_output_dir_arg,
)
from recidiviz.tools.looker.top_level_generators.base_lookml_generator import (
    LookMLGenerator,
)

BREADTH_AND_DEPTH_ATTRIBUTE_FIELDS = [
    "person_id",
    "event_date",
    "full_state_launch_date",
    "decarceral_impact_type",
    "has_mandatory_due_date",
    "is_jii_transition",
    "product_transition_type",
    "state_code",
    "system_type",
]

PARTITION_BY_COLUMNS = [
    "weight_factor",
    "delta_direction_factor",
    "person_id",
    "event_date",
    "decarceral_impact_type",
    "has_mandatory_due_date",
    "is_jii_transition",
]

EARLIEST_YEAR_FOR_BASELINE = 2023
EARLIEST_YEAR_FOR_BREADTH = 2024


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--save_views_to_dir",
        dest="save_dir",
        help="Specifies name of directory where to save view files",
        type=str,
        required=True,
    )

    return parser.parse_args()


def get_impact_transition_with_launch_order_query_fragment(
    partition_by_query_fragment: str,
) -> str:
    return f"""
    SELECT
      *,
        -- Record whether this transition is the one associated with the earliest full state launch of that task type for the selected attributes. If there are two launches of the same task type on the same day, the workflows launch gets priority
        DENSE_RANK() OVER(
            PARTITION BY {partition_by_query_fragment}{list_to_query_string(PARTITION_BY_COLUMNS)}
            ORDER BY DATE_TRUNC(DATE(full_state_launch_date), MONTH) ASC, (CASE WHEN product_transition_type IN ("workflows_discretionary","workflows_mandatory") THEN 1 ELSE 0 END) DESC, product_transition_type DESC
        ) AS full_state_launch_order
    FROM ${{impact_transition.SQL_TABLE_NAME}}
"""


def generate_baseline_source_view(
    attributes_query_fragment: str,
    partition_by_query_fragment: str,
) -> LookMLView:
    """Generates LookMLView for baseline source with selected attribute(s) for depth metrics calculation"""

    derived_table_query = f"""
-- Count weighted monthly transitions, deduped by person, event date, decarceral impact type, is jii transition, and has mandatory due date.
WITH impact_transition_with_launch_order AS (
    {get_impact_transition_with_launch_order_query_fragment(partition_by_query_fragment)}
),
transitions_by_month_weight_and_direction AS (
    SELECT
        {attributes_query_fragment}
        DATE_TRUNC(event_date, YEAR) AS transition_year,
        DATE_TRUNC(event_date, MONTH) AS transition_month,
        DATE_TRUNC(DATE(full_state_launch_date), MONTH) AS full_state_launch_month,
        weight_factor,
        delta_direction_factor,
        -- This count will be properly deduped as long as we enforce that all transitions with same person_id, event_date, decarceral_impact type, is_jii_transition, has_mandatory_due_date have the same value for weight_factor and the same value for delta_direction_factor.
        CAST(weight_factor AS FLOAT64) * COUNT(DISTINCT CONCAT(person_id, event_date, IFNULL(decarceral_impact_type, "NONE"), IFNULL(is_jii_transition, "NONE"), IFNULL(has_mandatory_due_date, "NONE"))) AS weighted_transitions,
    FROM
        impact_transition_with_launch_order
    WHERE
        EXTRACT(YEAR FROM event_date) >= {EARLIEST_YEAR_FOR_BASELINE}
        AND full_state_launch_order = 1
    GROUP BY 
        {attributes_query_fragment}transition_year, transition_month, DATE_TRUNC(DATE(full_state_launch_date), MONTH), weight_factor, delta_direction_factor
)
,
date_array AS (
    SELECT
        metric_month,
        DATE_TRUNC(metric_month, YEAR) as metric_year 
    FROM UNNEST(GENERATE_DATE_ARRAY("{EARLIEST_YEAR_FOR_BREADTH}-01-01", DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 MONTH), INTERVAL 1 MONTH)) AS metric_month
)
,
-- Baseline for old launches is just the average monthly transitions count
-- from the previous year
baseline_agg_old_launches AS (
    SELECT
        {attributes_query_fragment}
        metric_year,
        metric_month,
        weight_factor,
        delta_direction_factor,
        COALESCE(SUM(weighted_transitions)/12, 0) as monthly_baseline_old_tools
    FROM
        date_array
    LEFT JOIN
        transitions_by_month_weight_and_direction
    ON
        transition_month BETWEEN DATE_SUB(metric_year, INTERVAL 1 YEAR) AND DATE_SUB(metric_year, INTERVAL 1 DAY)
        AND full_state_launch_month < metric_year
    GROUP BY {attributes_query_fragment}metric_year, metric_month, weight_factor, delta_direction_factor
)
,
-- Baseline for new launches is the sum of monthly transitions for the year prior
-- to full state launch, among tools with a full state launch month falling before the 
-- metric month
baseline_agg_new_launches AS (
    SELECT
        {attributes_query_fragment}
        metric_year,
        metric_month,
        weight_factor,
        delta_direction_factor,
        COALESCE(SUM(IF(metric_month >= full_state_launch_month, weighted_transitions, 0))/12, 0) as monthly_baseline_new_tools,
        COUNT(DISTINCT IF(full_state_launch_month = metric_month, full_state_launch_month, NULL)) AS num_new_launches,
    FROM
        date_array
    LEFT JOIN
        transitions_by_month_weight_and_direction
    ON
        transition_month BETWEEN DATE_SUB(full_state_launch_month, INTERVAL 1 YEAR) AND DATE_SUB(full_state_launch_month, INTERVAL 1 DAY)
        AND full_state_launch_month BETWEEN metric_year AND DATE_ADD(metric_year, INTERVAL 1 YEAR)
    GROUP BY {attributes_query_fragment}metric_year, metric_month, weight_factor, delta_direction_factor
)
SELECT 
    {attributes_query_fragment}
    metric_year,
    metric_month,
    weight_factor,
    delta_direction_factor,
    IFNULL(num_new_launches, 0) AS num_new_launches,
    IFNULL(monthly_baseline_old_tools, 0) AS monthly_baseline_old_tools,
    IFNULL(monthly_baseline_new_tools, 0) AS monthly_baseline_new_tools,
    IFNULL(monthly_baseline_old_tools, 0) + IFNULL(monthly_baseline_new_tools, 0) AS total_baseline,
    ROW_NUMBER() OVER() as baseline_primary_key
FROM 
    baseline_agg_old_launches
FULL OUTER JOIN
    baseline_agg_new_launches
USING
    (
{attributes_query_fragment}metric_year, metric_month, weight_factor, delta_direction_factor
    )
ORDER BY {attributes_query_fragment}metric_year, metric_month, weight_factor, delta_direction_factor

    """

    dimensions = [
        DimensionLookMLViewField(
            field_name="metric_year",
            parameters=[
                LookMLFieldParameter.description("Metric Year"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.metric_year"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="metric_month",
            parameters=[
                LookMLFieldParameter.description("Metric Month"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.metric_month"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="weight_factor",
            parameters=[
                LookMLFieldParameter.description("Weight Factor"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("CAST(${TABLE}.weight_factor AS FLOAT64)"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="delta_direction_factor",
            parameters=[
                LookMLFieldParameter.description("Delta Direction Factor"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql(
                    "CAST(${TABLE}.delta_direction_factor AS INT64)"
                ),
            ],
        ),
        DimensionLookMLViewField(
            field_name="num_new_launches",
            parameters=[
                LookMLFieldParameter.description("Number of New Launches"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.num_new_launches"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="monthly_baseline_old_tools_dim",
            parameters=[
                LookMLFieldParameter.description("Monthly Baseline for Old Tools"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.monthly_baseline_old_tools"),
                LookMLFieldParameter.hidden(True),
            ],
        ),
        DimensionLookMLViewField(
            field_name="monthly_baseline_new_tools_dim",
            parameters=[
                LookMLFieldParameter.description("Monthly Baseline for New Tools"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.monthly_baseline_new_tools"),
                LookMLFieldParameter.hidden(True),
            ],
        ),
        DimensionLookMLViewField(
            field_name="total_baseline_dim",
            parameters=[
                LookMLFieldParameter.description("Total Baseline"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.total_baseline"),
                LookMLFieldParameter.hidden(True),
            ],
        ),
        DimensionLookMLViewField(
            field_name="baseline_primary_key",
            parameters=[
                LookMLFieldParameter.primary_key(True),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.sql("{TABLE}.baseline_primary_key"),
                LookMLFieldParameter.hidden(True),
            ],
        ),
    ]

    return LookMLView(
        view_name="transitions_baseline_metric",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*dimensions],
    )


def generate_transitions_breadth_metric_source_view(
    partition_by_query_fragment: str,
) -> LookMLView:
    """Generates LookMLView for transitions breadth metric source"""

    derived_table_query = f"""
WITH impact_transition_with_launch_order AS (
    {get_impact_transition_with_launch_order_query_fragment(partition_by_query_fragment)}
)
SELECT
    *,
    DATE_TRUNC(event_date, MONTH) AS metric_month,
    DATE_TRUNC(event_date, YEAR) AS metric_year,
    ROW_NUMBER() OVER() as breadth_primary_key
FROM
    impact_transition_with_launch_order
WHERE
    EXTRACT(YEAR FROM event_date) >= {EARLIEST_YEAR_FOR_BREADTH}
    AND event_date <= LAST_DAY(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 MONTH))
    AND is_during_or_after_full_state_launch_month = "true"
    """

    dimensions = [
        DimensionLookMLViewField(
            field_name="metric_month",
            parameters=[
                LookMLFieldParameter.description("Metric Month"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.metric_month"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="metric_month_truncated",
            parameters=[
                LookMLFieldParameter.description("Metric Month Truncated"),
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.sql("FORMAT_DATE('%Y-%m', ${TABLE}.metric_month)"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="metric_year",
            parameters=[
                LookMLFieldParameter.description("Metric Year"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.metric_year"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="weight_factor",
            parameters=[
                LookMLFieldParameter.description("Weight Factor"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("CAST(${TABLE}.weight_factor AS FLOAT64)"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="delta_direction_factor",
            parameters=[
                LookMLFieldParameter.description("Delta Direction Factor"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql(
                    "CAST(${TABLE}.delta_direction_factor AS INT64)"
                ),
            ],
        ),
        DimensionLookMLViewField(
            field_name="full_state_launch_order",
            parameters=[
                LookMLFieldParameter.description(
                    "Order of the full state launch this transition is associated with, with respect to selected attributes"
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Dimensions"),
                LookMLFieldParameter.sql("${TABLE}.full_state_launch_order"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="breadth_primary_key",
            parameters=[
                LookMLFieldParameter.primary_key(True),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.sql("{TABLE}.breadth_primary_key"),
                LookMLFieldParameter.hidden(True),
            ],
        ),
    ]

    for attribute in BREADTH_AND_DEPTH_ATTRIBUTE_FIELDS:
        if attribute in ["event_date", "full_state_launch_date"]:
            dimensions.append(
                DimensionLookMLViewField(
                    field_name=attribute,
                    parameters=[
                        LookMLFieldParameter.description(
                            f"{snake_to_title(attribute)}"
                        ),
                        LookMLFieldParameter.type(LookMLFieldType.DATE),
                        LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                        LookMLFieldParameter.view_label("Attributes"),
                        LookMLFieldParameter.sql(f"${{TABLE}}.{attribute}"),
                    ],
                )
            )
        elif attribute == "decarceral_impact_type":
            dimensions.append(
                DimensionLookMLViewField(
                    field_name=attribute,
                    parameters=[
                        LookMLFieldParameter.description(
                            f"{snake_to_title(attribute)}"
                        ),
                        LookMLFieldParameter.type(LookMLFieldType.STRING),
                        LookMLFieldParameter.view_label("Attributes"),
                        LookMLFieldParameter.sql(
                            f"INITCAP(REPLACE(${{TABLE}}.{attribute}, '_', ' '))"
                        ),
                    ],
                )
            )
        else:
            dimensions.append(
                DimensionLookMLViewField(
                    field_name=attribute,
                    parameters=[
                        LookMLFieldParameter.description(
                            f"{snake_to_title(attribute)}"
                        ),
                        LookMLFieldParameter.type(LookMLFieldType.STRING),
                        LookMLFieldParameter.view_label("Attributes"),
                        LookMLFieldParameter.sql(f"${{TABLE}}.{attribute}"),
                    ],
                )
            )

    return LookMLView(
        view_name="transitions_breadth_metric",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*dimensions],
    )


def generate_transitions_depth_metric_source_view(
    attributes_query_fragment: str, view_name: str
) -> LookMLView:
    """Generates LookMLView for transitions depth metric source"""

    derived_table_query = f"""
SELECT
*,
IF(DATE_TRUNC(DATE(full_state_launch_date),MONTH) = DATE_TRUNC(DATE(event_date), MONTH), CAST(weight_factor AS FLOAT64), CAST(0 AS FLOAT64)) as weight_factor_for_transition_added_via_launch,
CONCAT(person_id, event_date, IFNULL(decarceral_impact_type, "NONE"), IFNULL(is_jii_transition, "NONE"), IFNULL(has_mandatory_due_date, "NONE")) as distinct_key,
ROW_NUMBER() OVER () as depth_primary_key
FROM
    ${{transitions_baseline_metric.SQL_TABLE_NAME}}
LEFT JOIN 
    ${{transitions_breadth_metric.SQL_TABLE_NAME}}
USING
    (
    {attributes_query_fragment}metric_year, metric_month, weight_factor, delta_direction_factor
    )
    """

    dimensions = [
        DimensionLookMLViewField(
            field_name="breadth_primary_key",
            parameters=[
                LookMLFieldParameter.primary_key(False),
            ],
        ),
        DimensionLookMLViewField(
            field_name="baseline_primary_key",
            parameters=[
                LookMLFieldParameter.primary_key(False),
            ],
        ),
        DimensionLookMLViewField(
            field_name="depth_primary_key",
            parameters=[
                LookMLFieldParameter.primary_key(True),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.sql("{TABLE}.depth_primary_key"),
                LookMLFieldParameter.hidden(True),
            ],
        ),
    ]

    measures = [
        MeasureLookMLViewField(
            field_name="monthly_baseline_old_tools",
            parameters=[
                LookMLFieldParameter.label("Monthly Baseline Old Tools"),
                # Take the max "monthly baseline old tools" of all observations in the group, which should be equivalent to the "monthly baseline old tools", assuming all "monthly baseline old tools" values are the same for every member of the group.
                # This measure definition assumes observations are grouped by metric_month, metric_year, delta_direction_factor, weight_factor,
                # state_code, product_transition_type, decarceral_impact_type, has_mandatory_due_date, and is_jii_transition prior to aggregation.
                LookMLFieldParameter.type(LookMLFieldType.MAX),
                LookMLFieldParameter.description("Monthly baseline old tools"),
                LookMLFieldParameter.view_label("Metrics"),
                LookMLFieldParameter.sql("${monthly_baseline_old_tools_dim}"),
            ],
        ),
        MeasureLookMLViewField(
            field_name="monthly_baseline_new_tools",
            parameters=[
                LookMLFieldParameter.label("Monthly Baseline New Tools"),
                # Take the max "monthly baseline new tools" of all observations in the group, which should be equivalent to the "monthly baseline new tools", assuming all "monthly baseline new tools" values are the same for every member of the group.
                # This measure definition assumes observations are grouped by metric_month, metric_year, delta_direction_factor, weight_factor,
                # state_code, product_transition_type, decarceral_impact_type, has_mandatory_due_date, and is_jii_transition prior to aggregation.
                LookMLFieldParameter.type(LookMLFieldType.MAX),
                LookMLFieldParameter.description("Monthly baseline new tools"),
                LookMLFieldParameter.view_label("Metrics"),
                LookMLFieldParameter.sql("${monthly_baseline_new_tools_dim}"),
            ],
        ),
        MeasureLookMLViewField(
            field_name="total_baseline",
            parameters=[
                LookMLFieldParameter.label("Total Baseline"),
                # Take the max "total baseline" of all observations in the group, which should be equivalent to the "total baseline", assuming all "total baseline" values are the same for every member of the group.
                # This measure definition assumes observations are grouped by metric_month, metric_year, delta_direction_factor, weight_factor,
                # state_code, product_transition_type, decarceral_impact_type, has_mandatory_due_date, and is_jii_transition prior to aggregation.
                LookMLFieldParameter.type(LookMLFieldType.MAX),
                LookMLFieldParameter.description("Total baseline"),
                LookMLFieldParameter.view_label("Metrics"),
                LookMLFieldParameter.sql("${total_baseline_dim}"),
            ],
        ),
        MeasureLookMLViewField(
            field_name="transitions",
            parameters=[
                LookMLFieldParameter.label("Transitions"),
                LookMLFieldParameter.type(LookMLFieldType.SUM_DISTINCT),
                LookMLFieldParameter.description(
                    "Counts transitions, applying weight factors"
                ),
                LookMLFieldParameter.view_label("Metrics"),
                LookMLFieldParameter.sql_distinct_key("${TABLE}.distinct_key"),
                # This will throw an error if there is ever more than one weight_factor for multiple transitions with the same person_id, event_date, decarceral_impact_type, is_jii_transition, and has_mandatory_due_date
                LookMLFieldParameter.sql("${weight_factor}"),
            ],
        ),
        MeasureLookMLViewField(
            field_name="new_transitions_added_via_launch",
            parameters=[
                LookMLFieldParameter.label("New Transitions Added Via Launch"),
                LookMLFieldParameter.type(LookMLFieldType.SUM_DISTINCT),
                LookMLFieldParameter.description(
                    "Counts transitions associated with a launch in the same month, applying weight factors. Excludes supervisor homepage opportunities module launches."
                ),
                LookMLFieldParameter.view_label("Metrics"),
                LookMLFieldParameter.sql_distinct_key("${TABLE}.distinct_key"),
                # This will throw an error if there is ever more than one weight_factor for multiple transitions with the same person_id, event_date, decarceral_impact_type, is_jii_transition, and has_mandatory_due_date
                LookMLFieldParameter.sql(
                    "${TABLE}.weight_factor_for_transition_added_via_launch"
                ),
                LookMLFieldParameter.filters(
                    [
                        (
                            "product_transition_type",
                            "-supervisor_opportunities_module_discretionary AND -supervisor_opportunities_module_mandatory",
                        )
                    ]
                ),
            ],
        ),
        MeasureLookMLViewField(
            field_name="transitions_delta_without_direction_factor",
            parameters=[
                LookMLFieldParameter.label(
                    "Transitions Delta (Without Direction Factor)"
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.description(
                    "Calculates percent change in transitions with weight factors applied"
                ),
                LookMLFieldParameter.view_label("Metrics"),
                LookMLFieldParameter.sql(
                    "ROUND(SAFE_DIVIDE((${transitions} - ${total_baseline}), ${total_baseline}), 4)"
                ),
            ],
        ),
    ]

    return LookMLView(
        view_name="transitions_depth_metric",
        included_paths=[f"/views/{view_name}/generated/*"],
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        drill_fields=[
            "person_id",
            "event_date",
            "full_state_launch_date",
            "state_code",
            "system_type",
            "product_transition_type",
            "decarceral_impact_type",
            "has_mandatory_due_date",
            "is_jii_transition",
            "delta_direction_factor",
            "weight_factor",
        ],
        fields=[
            *dimensions,
            *measures,
        ],
        extended_views=[
            "transitions_breadth_metric",
            "transitions_baseline_metric",
        ],
    )


def main(
    output_directory: str,
    view_name: str,
) -> None:
    """Builds and writes breadth and depth metrics views in Looker"""
    attributes_query_fragment = "".join(
        [
            liquid_wrap_json_field(f"{attribute},", attribute, view_name)
            for attribute in BREADTH_AND_DEPTH_ATTRIBUTE_FIELDS
        ]
    )

    partition_by_query_fragment = "".join(
        [
            liquid_wrap_json_field(f"{attribute},", attribute, view_name)
            for attribute in BREADTH_AND_DEPTH_ATTRIBUTE_FIELDS
            if attribute not in PARTITION_BY_COLUMNS
        ]
    )

    generate_baseline_source_view(
        attributes_query_fragment,
        partition_by_query_fragment,
    ).write(output_directory, source_script_path=__file__)

    generate_transitions_breadth_metric_source_view(
        partition_by_query_fragment,
    ).write(output_directory, source_script_path=__file__)

    generate_transitions_depth_metric_source_view(
        attributes_query_fragment,
        view_name,
    ).write(output_directory, source_script_path=__file__)


class BreadthAndDepthLookMLGenerator(LookMLGenerator):
    """Generates LookML files for breadth and depth metrics views."""

    @staticmethod
    def generate_lookml(output_dir: str) -> None:
        """
        Write breadth and depth metrics LookML views to the given directory,
        which should be a path to the local copy of the looker repo
        """
        view_name = "breadth_and_depth"

        output_subdir = get_generated_views_path(
            output_dir=output_dir, module_name=view_name
        )

        main(
            output_directory=output_subdir,
            view_name=view_name,
        )


if __name__ == "__main__":
    BreadthAndDepthLookMLGenerator.generate_lookml(
        output_dir=parse_and_validate_output_dir_arg()
    )
