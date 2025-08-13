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
"""Observation view configuration."""
from typing import Sequence

from more_itertools import one

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import (
    PULSE_DASHBOARD_SEGMENT_DATASET,
)
from recidiviz.segment.segment_event_big_query_view_builder import (
    SegmentEventBigQueryViewBuilder,
)
from recidiviz.segment.segment_product_event_big_query_view_builder import (
    SegmentProductEventBigQueryViewBuilder,
)
from recidiviz.segment.segment_product_event_big_query_view_collector import (
    SegmentProductEventBigQueryViewCollector,
)
from recidiviz.source_tables.externally_managed.collect_externally_managed_source_table_configs import (
    collect_externally_managed_source_table_collections,
)


# TODO(#46240): Reframe this as `_get_product_specific_attribute_columns_statement`
# and rename / restructure columns_to_include_in_unioned_segment_view to something like
# get_product_specific_columns.
def _get_shared_columns_statement(
    vb: SegmentProductEventBigQueryViewBuilder,
) -> str:
    """Generates a SQL statement for shared columns in unioned segment event views
    that fills in NULL for columns not present in the specific segment event view."""

    if not vb.product_type.columns_to_include_in_unioned_segment_view:
        return ""

    shared_columns: list[str] = []
    for col in vb.product_type.columns_to_include_in_unioned_segment_view:
        if vb.additional_attribute_cols and (col in vb.additional_attribute_cols):
            shared_columns.append(col)
        else:
            shared_columns.append(f"CAST(NULL AS STRING) AS {col}")
    return list_to_query_string(shared_columns)


def _get_unioned_segment_event_builders() -> list[UnionAllBigQueryViewBuilder]:
    """Generates a list of UnionAllBigQueryViewBuilder instances for each product type"""

    def _select_statement(vb: SegmentProductEventBigQueryViewBuilder) -> str:
        return f"""
SELECT
    state_code,
    email,
    "{vb.segment_event_name}" AS event,
    event_ts,
    person_id,
    context_page_path,
    {_get_shared_columns_statement(vb)}
"""

    product_union_builders = []
    for product_type, builders in (
        SegmentProductEventBigQueryViewCollector()
        .collect_segment_product_event_view_builders_by_product()
        .items()
    ):
        view_id = f"all_{product_type.pretty_name}_segment_events"
        product_dataset_id = builders[0].address.dataset_id
        unioned_builder = UnionAllBigQueryViewBuilder(
            dataset_id=product_dataset_id,
            view_id=view_id,
            description="Union of all segment events for a product.",
            parents=builders,
            clustering_fields=["state_code", "person_id"],
            parent_to_select_statement=_select_statement,
        )
        product_union_builders.append(unioned_builder)
    return product_union_builders


def _get_segment_event_view_builders() -> Sequence[SegmentEventBigQueryViewBuilder]:
    """Generates a sequence of SegmentEventBigQueryViewBuilder instances for each
    segment event view. Excludes `pages` and `identifies` events as logins and pageviews
    do not count toward active usage events.
    """
    segment_event_source_table_addresses = one(
        c
        for c in collect_externally_managed_source_table_collections(project_id=None)
        if c.dataset_id == PULSE_DASHBOARD_SEGMENT_DATASET
    ).source_tables_by_address

    # TODO(#46239): Derive segment_table_jii_pseudonymized_id_columns/additional_attribute_cols
    #  directly from an event-specific config and remove this
    product_event_builders_by_source_table_address = (
        SegmentProductEventBigQueryViewCollector().collect_segment_product_event_view_builders_by_event_source_table_address()
    )

    event_view_builders = []

    for event_source_table_address in segment_event_source_table_addresses:
        if event_source_table_address.table_id not in ["identifies", "pages"]:
            product_event_builders = product_event_builders_by_source_table_address[
                event_source_table_address
            ]
            segment_table_jii_pseudonymized_id_columns = sorted(
                {
                    col
                    for b in product_event_builders
                    for col in (b.segment_table_jii_pseudonymized_id_columns or [])
                }
            )
            additional_attribute_cols = sorted(
                {
                    col
                    for b in product_event_builders
                    for col in (b.additional_attribute_cols or [])
                }
            )

            event_view_builders.append(
                SegmentEventBigQueryViewBuilder(
                    description=f"Segment event view for {event_source_table_address.table_id} events",
                    segment_events_source_table_address=event_source_table_address,
                    segment_table_jii_pseudonymized_id_columns=segment_table_jii_pseudonymized_id_columns,
                    additional_attribute_cols=additional_attribute_cols,
                )
            )
    return event_view_builders


def _get_unioned_segment_event_view_builder() -> UnionAllBigQueryViewBuilder:
    """Generates a UnionAllBigQueryViewBuilder for all segment events."""
    return UnionAllBigQueryViewBuilder(
        dataset_id="segment_events",
        view_id="all_segment_events",
        description="Union of all segment events across products.",
        parents=_get_segment_event_view_builders(),
        clustering_fields=["state_code", "email"],
        parent_to_select_statement=lambda vb: f"""
SELECT
    state_code,
    email,
    "{vb.segment_event_name}" AS event,
    event_ts,
    person_id,
    context_page_path,
    product_type,
""",
    )


def get_view_builders_for_views_to_update() -> Sequence[BigQueryViewBuilder]:
    return [
        # TODO(#46239): Replace SegmentProductEventBigQueryViewCollector /
        #  SegmentProductEventBigQuery with simple fn that loops over all
        #  (event, product_type) pairs and simply selects from the appropriate
        #  event-level with a product_type filter.
        *SegmentProductEventBigQueryViewCollector().collect_view_builders(),
        *_get_unioned_segment_event_builders(),
        *_get_segment_event_view_builders(),
        _get_unioned_segment_event_view_builder(),
    ]
