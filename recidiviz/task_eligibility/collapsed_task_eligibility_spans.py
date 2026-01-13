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
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
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
        # Query format args
        completion_event_type=completion_event_type,
        tes_view_materialized_address=tes_builder.table_for_query.to_str(),
    )
