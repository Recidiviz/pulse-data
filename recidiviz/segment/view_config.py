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
"""Segment view configuration."""
from collections import defaultdict
from typing import Sequence

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.segment.all_segment_pages import ALL_SEGMENT_PAGES_VIEW_BUILDER
from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_big_query_view_builder import (
    SegmentEventBigQueryViewBuilder,
)
from recidiviz.segment.segment_event_big_query_view_collector import (
    SegmentEventBigQueryViewCollector,
)


def _get_unioned_product_specific_segment_event_builders(
    event_builders: Sequence[SegmentEventBigQueryViewBuilder],
) -> list[SimpleBigQueryViewBuilder]:
    """Generates a list of view builders for unioned segment events by product type.

    These union views query event-level views with product_type filters.
    """
    # Group event builders by product type
    event_builders_by_product: dict[
        ProductType, list[SegmentEventBigQueryViewBuilder]
    ] = defaultdict(list)

    for event_builder in event_builders:
        for product_type in event_builder.relevant_product_types:
            event_builders_by_product[product_type].append(event_builder)

    # Create union view for each product
    product_union_builders = []
    for product_type, builders in event_builders_by_product.items():
        view_id = f"all_{product_type.pretty_name}_segment_events"

        # Get product-specific columns that should be included in the union view
        product_specific_cols = product_type.columns_to_include_in_unioned_segment_view

        ctes = []
        for builder in sorted(builders, key=lambda b: b.segment_event_name):
            event_table_address = builder.table_for_query

            # Build the SELECT columns
            select_cols = [
                "state_code",
                "email_address",
                f'"{builder.segment_event_name}" AS event',
                "event_ts",
                "person_id",
                "context_page_path",
                "session_id",
            ]

            for col in product_specific_cols:
                if (
                    builder.additional_attribute_cols
                    and col in builder.additional_attribute_cols
                ):
                    # Include the actual column from the event
                    select_cols.append(col)
                else:
                    # Include NULL placeholder for schema consistency
                    select_cols.append(f"CAST(NULL AS STRING) AS {col}")

            # Join all columns with commas
            columns_statement = ",\n    ".join(select_cols)

            ctes.append(
                f"""
SELECT
    {columns_statement}
FROM `{event_table_address.format_address_for_query_template()}`
WHERE product_type = "{product_type.value}"
"""
            )

        view_query_template = "\nUNION ALL\n".join(ctes)

        unioned_builder = SimpleBigQueryViewBuilder(
            dataset_id="segment_events",
            view_id=view_id,
            description=f"Union of all segment events for {product_type.display_name}.",
            view_query_template=view_query_template,
            should_materialize=True,
            clustering_fields=["state_code", "person_id"],
        )
        product_union_builders.append(unioned_builder)
    return product_union_builders


def _get_unioned_segment_event_view_builder(
    parents: Sequence[SegmentEventBigQueryViewBuilder],
) -> UnionAllBigQueryViewBuilder:
    """Generates a UnionAllBigQueryViewBuilder for all segment events."""
    return UnionAllBigQueryViewBuilder(
        dataset_id="segment_events",
        view_id="all_segment_events",
        description="Union of all segment events across products.",
        parents=parents,
        clustering_fields=["state_code", "email_address"],
        parent_view_to_select_statement=lambda vb: f"""
SELECT
    state_code,
    email_address,
    "{vb.segment_event_name}" AS event,
    event_ts,
    person_id,
    context_page_path,
    product_type,
    session_id,
""",
    )


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    event_level_view_builders = (
        SegmentEventBigQueryViewCollector().collect_view_builders()
    )
    return [
        # Event-level views
        *event_level_view_builders,
        # Union of all event-level views
        _get_unioned_segment_event_view_builder(event_level_view_builders),
        # Union of all events for a single product (queries event-level views with product filters)
        *_get_unioned_product_specific_segment_event_builders(
            event_level_view_builders
        ),
        # Pages view
        ALL_SEGMENT_PAGES_VIEW_BUILDER,
    ]
