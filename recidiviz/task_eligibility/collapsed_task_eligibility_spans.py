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
"""Builds sessionized views that collapse TES spans into spans where is_eligible /
is_almost_eligible have the same value.
"""
import attrs

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_column import (
    BigQueryViewColumn,
    Integer,
    String,
)
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
    single_task_eligibility_span_schema,
)
from recidiviz.utils.string import StrictStringFormatter

_COLLAPSED_TASK_ELIGIBILITY_SPANS_QUERY_TEMPLATE = f"""
WITH spans_with_type AS (
    SELECT
        state_code,
        person_id,
        task_name,
        "{{completion_event_type}}" AS task_type,
        start_date,
        end_date,
        is_eligible,
        is_almost_eligible,
    FROM `{{project_id}}.{{tes_view_materialized_address}}`
),
sessionized_cte AS (
{aggregate_adjacent_spans(table_name='spans_with_type',
                   attribute=['task_name', 'task_type', 'is_eligible', 'is_almost_eligible'],
                   session_id_output_name='task_eligibility_span_id',
                   end_date_field_name='end_date')}
)
SELECT
    *,
FROM sessionized_cte
"""

_VIEW_DESCRIPTION_TEMPLATE = """
Sessionized view of `{base_task_address_str}` that collapses spans
based on the `task_name`, `is_eligible` and `is_almost_eligible` fields for each client.
"""


def collapsed_task_eligibility_span_schema() -> list[BigQueryViewColumn]:
    """Returns the schema for a collapsed task eligibility spans view. Column
    order must match the SELECT output of aggregate_adjacent_spans (which
    outputs person_id, state_code first) wrapped by ``SELECT *`` in the query
    template, because BQ materialization maps columns by position."""
    single_cols = {col.name: col for col in single_task_eligibility_span_schema()}
    return [
        single_cols["person_id"],
        single_cols["state_code"],
        attrs.evolve(
            single_cols["task_eligibility_span_id"],
            description="An ID for this collapsed task eligibility span which is unique to a `person_id` and can be used to order across all eligibility spans for a person",
        ),
        Integer(
            name="date_gap_id",
            description="A numerical id that, for ordered spans associated with a given `person_id`, remains the same for consecutive spans with no date gap between them and is incremented by one every time there is a date gap between consecutive spans",
            mode="NULLABLE",
        ),
        attrs.evolve(
            single_cols["start_date"],
            description="The start date of the collapsed eligibility span (inclusive).",
        ),
        attrs.evolve(
            single_cols["end_date"],
            description="The exclusive end date of the collapsed eligibility span, or null if the span is still open.",
        ),
        single_cols["task_name"],
        String(
            name="task_type",
            description="The completion event type for this task.",
            mode="REQUIRED",
        ),
        single_cols["is_eligible"],
        attrs.evolve(
            single_cols["is_almost_eligible"],
            description="Whether the person is almost eligible for the task during this span, as defined by this task's specific 'almost eligibility' configuration logic",
        ),
    ]


def _build_collapsed_tes_spans_view_address(
    tes_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
) -> BigQueryAddress:
    return BigQueryAddress(
        dataset_id=tes_builder.dataset_id, table_id=f"{tes_builder.view_id}__collapsed"
    )


def build_collapsed_tes_spans_view_materialized_address(
    tes_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
) -> BigQueryAddress:
    view_address = _build_collapsed_tes_spans_view_address(tes_builder)
    return BigQueryViewBuilder.build_standard_materialized_address(
        dataset_id=view_address.dataset_id,
        view_id=view_address.table_id,
    )


def build_collapsed_task_eligibility_spans_view_for_tes_builder(
    tes_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
) -> SimpleBigQueryViewBuilder:
    view_address = _build_collapsed_tes_spans_view_address(tes_builder)
    completion_event_type = (
        tes_builder.completion_event_builder.completion_event_type.name
    )
    return SimpleBigQueryViewBuilder(
        dataset_id=view_address.dataset_id,
        view_id=view_address.table_id,
        view_query_template=_COLLAPSED_TASK_ELIGIBILITY_SPANS_QUERY_TEMPLATE,
        description=StrictStringFormatter().format(
            _VIEW_DESCRIPTION_TEMPLATE,
            base_task_address_str=tes_builder.address.to_str(),
        ),
        clustering_fields=["state_code", "person_id"],
        should_materialize=True,
        schema=collapsed_task_eligibility_span_schema(),
        # Query format args
        completion_event_type=completion_event_type,
        tes_view_materialized_address=tes_builder.table_for_query.to_str(),
    )
