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
"""Tests for segment_product_event_big_query_view_builder.py"""
import unittest

from recidiviz.segment.product_type import ProductType
from recidiviz.segment.segment_event_config import (
    SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS,
    get_product_types_for_event,
)
from recidiviz.segment.segment_product_event_big_query_view_collector import (
    SegmentProductEventBigQueryViewCollector,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_addresses,
)


class SegmentProductEventBigQueryViewBuilderTest(unittest.TestCase):
    """Tests for segment_product_event_big_query_view_builder.py"""

    def test_segment_event_source_table_is_valid_source_table_address(self) -> None:
        """Test that the segment_event_source_table is a valid source table address for all segment event builders."""
        valid_source_table_addresses = get_all_source_table_addresses()
        for (
            builder
        ) in SegmentProductEventBigQueryViewCollector().collect_view_builders():
            if builder.segment_table_sql_source not in valid_source_table_addresses:
                raise ValueError(
                    f"Invalid source table `{builder.segment_table_sql_source.to_str()}` "
                    f"found as segment_event_source_table for `{builder.view_id}` "
                    f"{builder.product_name} segment event builder.",
                )

    def test_segment_event_source_dataset_is_segment_dataset(self) -> None:
        """Test that the segment_event_source_table references a *segment_metrics dataset."""
        for (
            builder
        ) in SegmentProductEventBigQueryViewCollector().collect_view_builders():
            if not builder.segment_table_sql_source.dataset_id.endswith(
                "_segment_metrics"
            ):
                raise ValueError(
                    f"Invalid segment_event_source_table `{builder.segment_table_sql_source.to_str()}`, "
                    f"expected *_segment_metrics dataset, found `{builder.segment_table_sql_source.dataset_id}`.",
                )

    # TODO(#46240): Delete this test once product X event views are directly derived
    #   from SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS.
    def test_matches_segment_event_name_to_relevant_products(self) -> None:
        """Test that every view builder is aligned with the correct product types as
        designated in SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS."""
        configured_segment_events_to_products: dict[str, list[ProductType]] = {}
        for (
            builder
        ) in SegmentProductEventBigQueryViewCollector().collect_view_builders():
            # assemble dictionary mapping segment event names to relevant products
            if builder.segment_event_name not in configured_segment_events_to_products:
                configured_segment_events_to_products[builder.segment_event_name] = []
            configured_segment_events_to_products[builder.segment_event_name].append(
                builder.product_type
            )

        # cross-check configured_segment_events_to_products with SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS
        if set(configured_segment_events_to_products) != set(
            SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS
        ):
            raise ValueError(
                f"Mismatch between configured segment event names {set(configured_segment_events_to_products)} "
                f"and SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS {set(SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS)}."
            )
        for (
            event_name,
            relevant_products,
        ) in configured_segment_events_to_products.items():
            expected_relevant_products = get_product_types_for_event(event_name)
            if set(relevant_products) != set(expected_relevant_products):
                raise ValueError(
                    f"Mismatch for segment event name `{event_name}`: "
                    f"configured products in SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS {relevant_products} do not match "
                    f"view builders configured for these products: {expected_relevant_products}.",
                )
