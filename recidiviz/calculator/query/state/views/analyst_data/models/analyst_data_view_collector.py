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
"""Functions for collating span or event query builders into a view builder."""
from typing import List, Union

from pytablewriter import MarkdownTableWriter

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.models.event_query_builder import (
    EventQueryBuilder,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.person_events import (
    PERSON_EVENTS,
)
from recidiviz.calculator.query.state.views.analyst_data.models.person_spans import (
    PERSON_SPANS,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_query_builder import (
    SpanQueryBuilder,
)


def _get_query_builder_properties_markdown_table(
    query_builder_type: str,
    query_builders: Union[List[EventQueryBuilder], List[SpanQueryBuilder]],
) -> str:
    table_rows = []
    for q in query_builders:
        table_rows.append((q.pretty_name, q.description, f"`{q.attribute_cols}`"))

    writer = MarkdownTableWriter(
        headers=[query_builder_type, "Description", "JSON attributes"],
        value_matrix=table_rows,
        margin=0,
    )
    return writer.dumps()


def generate_unioned_view_builder(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    query_builders: Union[List[EventQueryBuilder], List[SpanQueryBuilder]],
) -> SimpleBigQueryViewBuilder:
    """Unions together a list of span or event query builders into a BigQuery view builder. All
    query builders must produce a query containing the index columns that define the specified
    `unit_of_analysis_type`."""
    if not (
        all(isinstance(x, EventQueryBuilder) for x in query_builders)
        or all(isinstance(x, SpanQueryBuilder) for x in query_builders)
    ):
        raise ValueError(
            "All query builders in list must be of the same type (either EventQueryBuilder or SpanQueryBuilder)."
        )
    query_builder_type_label = query_builders[0].query_builder_label
    unit_of_analysis_type_label = unit_of_analysis_type.value.lower()

    view_id = f"{unit_of_analysis_type_label}_{query_builder_type_label}s"
    view_description_header = f"View concatenating {query_builder_type_label} queries at the {unit_of_analysis_type_label} level in a common format."
    view_description = f"""
{view_description_header}

To change or add attributes to an existing entity, see `{unit_of_analysis_type_label}_{query_builder_type_label}s.py`.
To create a new `{unit_of_analysis_type_label}_{query_builder_type_label}`, add a new enum for the new {query_builder_type_label} to `{unit_of_analysis_type_label}_{query_builder_type_label}_type.py` and configure in `{unit_of_analysis_type_label}_{query_builder_type_label}s.py`.

{_get_query_builder_properties_markdown_table(query_builder_type_label.title(), query_builders)}

"""

    query_template = "\nUNION ALL\n".join(
        [q.generate_subquery(unit_of_analysis_type) for q in query_builders]
    )
    return SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        bq_description=view_description_header,
        should_materialize=True,
        clustering_fields=["state_code", query_builder_type_label],
    )


def get_person_spans_and_events_view_builders() -> List[SimpleBigQueryViewBuilder]:
    """Returns all view builders for configured spans and events"""
    return [
        generate_unioned_view_builder(
            unit_of_analysis_type=MetricUnitOfAnalysisType.PERSON_ID,
            query_builders=PERSON_SPANS,
        ),
        generate_unioned_view_builder(
            unit_of_analysis_type=MetricUnitOfAnalysisType.PERSON_ID,
            query_builders=PERSON_EVENTS,
        ),
    ]
