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
"""Tests for segment_event_big_query_view_collector.py"""
import unittest

from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.segment.segment_event_big_query_view_builder import (
    SegmentEventBigQueryViewBuilder,
)
from recidiviz.segment.segment_event_big_query_view_collector import (
    SegmentEventBigQueryViewCollector,
)
from recidiviz.segment.segment_event_utils import SEGMENT_DATASETS
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_addresses,
)
from recidiviz.source_tables.externally_managed.collect_externally_managed_source_table_configs import (
    collect_externally_managed_source_table_collections,
)


class SegmentEventBigQueryViewCollectorTest(unittest.TestCase):
    """Tests for segment_event_big_query_view_collector.py"""

    def test_all_builders_are_segment_event_builders(self) -> None:
        """Test that all collected builders are SegmentEventBigQueryViewBuilder instances."""
        collector = SegmentEventBigQueryViewCollector()
        builders = collector.collect_view_builders()

        for builder in builders:
            self.assertIsInstance(builder, SegmentEventBigQueryViewBuilder)

    def test_segment_event_source_table_is_valid_source_table_address(self) -> None:
        """Test that the segment_event_source_table is a valid source table address for all segment event builders."""
        valid_source_table_addresses = get_all_source_table_addresses()
        collector = SegmentEventBigQueryViewCollector()

        for builder in collector.collect_view_builders():
            if builder.segment_table_sql_source not in valid_source_table_addresses:
                raise ValueError(
                    f"Invalid source table `{builder.segment_table_sql_source.to_str()}` "
                    f"found as segment_event_source_table for `{builder.view_id}` "
                    f"segment event builder."
                )

    def test_segment_event_source_dataset_is_segment_dataset(self) -> None:
        """Test that the segment_event_source_table references a valid segment dataset."""
        collector = SegmentEventBigQueryViewCollector()

        for builder in collector.collect_view_builders():
            if builder.segment_table_sql_source.dataset_id not in SEGMENT_DATASETS:
                raise ValueError(
                    f"Invalid segment_event_source_table `{builder.segment_table_sql_source.to_str()}`, "
                    f"expected one of {SEGMENT_DATASETS}, found `{builder.segment_table_sql_source.dataset_id}`."
                )

    def test_all_segment_source_tables_have_corresponding_view_builders(self) -> None:
        """
        Validates that every YAML source table (except 'identifies' and 'pages') has a corresponding view builder
        """
        # Collect source tables from all segment event datasets
        segment_event_source_table_addresses: set[BigQueryAddress] = set()
        for dataset_id in SEGMENT_DATASETS:
            try:
                collection = one(
                    c
                    for c in collect_externally_managed_source_table_collections(
                        project_id=None
                    )
                    if c.dataset_id == dataset_id
                )
                for address in collection.source_tables_by_address:
                    segment_event_source_table_addresses.add(address)
            except ValueError:
                # Dataset doesn't exist, skip
                pass

        # Collect view builders
        collector = SegmentEventBigQueryViewCollector()
        event_builders = collector.collect_view_builders()

        # Build set of addresses from view builders
        view_builder_addresses = {
            builder.segment_table_sql_source for builder in event_builders
        }

        # Build set of addresses from YAMLs (excluding 'identifies' and 'pages')
        yaml_addresses = {
            address
            for address in segment_event_source_table_addresses
            if address.table_id not in ["identifies", "pages"]
        }

        # Check for YAMLs without view builders
        yaml_without_view = yaml_addresses - view_builder_addresses
        if yaml_without_view:
            raise ValueError(
                f"Found segment event source tables in YAML without corresponding view builders: "
                f"{sorted(addr.to_str() for addr in yaml_without_view)}"
            )
