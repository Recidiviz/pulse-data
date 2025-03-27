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
"""Function that calculates the breadth (transitions count) metric for a given year
and set of disaggregating attributes."""

from typing import Optional

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import TRANSITIONS_DATASET_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent

DISTINCT_COUNT_COLUMNS = [
    "person_id",
    "event_date",
    "decarceral_impact_type",
    "has_mandatory_due_date",
    "is_jii_transition",
]


def get_transitions_breadth_metric_for_year(
    metric_year: int = 2024,
    attribute_cols: Optional[list[str]] = None,
) -> SimpleBigQueryViewBuilder:
    """Returns a view builder that represents the monthly transitions from
    fully launched tools for the metric year, along with a count of new transitions
    introduced by a full state launch occurring in the metric month. If attribute_cols
    are provided, the view will be disaggregated by those attributes.
    View is always disaggregated by weight factor and delta direction factor."""
    attribute_cols_with_defaults = (attribute_cols if attribute_cols else []) + [
        "weight_factor",
        "delta_direction_factor",
    ]
    group_by_columns_query_fragment = (
        fix_indent(",\n".join(attribute_cols_with_defaults), indent_level=8) + ",\n"
    )

    query_template = f"""
SELECT
    DATE_TRUNC(event_date, MONTH) AS metric_month,{group_by_columns_query_fragment}
    COUNT(DISTINCT CONCAT({list_to_query_string(DISTINCT_COUNT_COLUMNS)})) AS transitions,
    COUNT(DISTINCT 
        IF(
            -- Count distinct transitions occurring during the month of their launch, excluding supervisor homepage opportunities module launches
            (DATE_TRUNC(DATE(full_state_launch_date), MONTH) = DATE_TRUNC(event_date, MONTH) AND product_transition_type NOT IN ("supervisor_opportunities_module_discretionary", "supervisor_opportunities_module_mandatory")),
            CONCAT({list_to_query_string(DISTINCT_COUNT_COLUMNS)}),
            NULL
        )
    ) AS new_transitions_added_via_launch,
FROM
    `{{project_id}}.observations__person_event.impact_transition_materialized`
WHERE
    EXTRACT(YEAR FROM event_date) = {metric_year}
    AND event_date <= CURRENT_DATE('US/Eastern')
    AND is_during_or_after_full_state_launch_month = "true"
GROUP BY
{group_by_columns_query_fragment}metric_month
ORDER BY
{group_by_columns_query_fragment}metric_month
"""
    view_id_tag = "__" + "_".join(attribute_cols) if attribute_cols else ""

    view_id = f"transitions_breadth_metric_{metric_year}{view_id_tag}"
    view_description = (
        f"Monthly transitions for fully launched tools, for year {metric_year}"
    )

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
        get_transitions_breadth_metric_for_year(
            2024, ["product_transition_type"]
        ).build_and_print()
