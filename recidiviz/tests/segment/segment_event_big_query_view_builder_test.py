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
"""Tests for segment_event_big_query_view_builder.py"""
import unittest

from recidiviz.segment.segment_event_big_query_view_collector import (
    SegmentEventBigQueryViewCollector,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_addresses,
)


class SegmentEventBigQueryViewBuilderTest(unittest.TestCase):
    def test_segment_event_source_table_is_valid_source_table_address(self) -> None:
        """Test that the segment_event_source_table is a valid source table address for all segment event builders."""
        for builder in SegmentEventBigQueryViewCollector().collect_view_builders():
            if builder.segment_table_sql_source not in get_all_source_table_addresses():
                raise ValueError(
                    f"Invalid source table `{builder.segment_table_sql_source.to_str()}` "
                    f"found as segment_event_source_table for `{builder.view_id}` "
                    f"{builder.product_name} segment event builder.",
                )

    def test_segment_event_source_dataset_is_segment_dataset(self) -> None:
        """Test that the segment_event_source_table references a *segment_metrics dataset."""
        for builder in SegmentEventBigQueryViewCollector().collect_view_builders():
            if not builder.segment_table_sql_source.dataset_id.endswith(
                "_segment_metrics"
            ):
                raise ValueError(
                    f"Invalid segment_event_source_table `{builder.segment_table_sql_source.to_str()}`, "
                    f"expected *_segment_metrics dataset, found `{builder.segment_table_sql_source.dataset_id}`.",
                )
