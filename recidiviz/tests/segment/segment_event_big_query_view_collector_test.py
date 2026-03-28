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

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.segment.segment_event_big_query_view_builder import (
    SegmentEventBigQueryViewBuilder,
)
from recidiviz.segment.segment_event_big_query_view_collector import (
    SegmentEventBigQueryViewCollector,
)
from recidiviz.segment.segment_event_utils import (
    SEGMENT_FRONTEND_TRACKING_DATASETS,
    get_segment_frontend_event_source_table_addresses,
)
from recidiviz.segment.view_config import get_view_builders_for_views_to_update
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_addresses,
)
from recidiviz.source_tables.untracked_source_table_exemptions import (
    get_allowed_tables_in_source_table_datasets_with_no_config,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


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
            if (
                builder.segment_table_sql_source.dataset_id
                not in SEGMENT_FRONTEND_TRACKING_DATASETS
            ):
                raise ValueError(
                    f"Invalid segment_event_source_table `{builder.segment_table_sql_source.to_str()}`, "
                    f"expected one of {SEGMENT_FRONTEND_TRACKING_DATASETS}, found `{builder.segment_table_sql_source.dataset_id}`."
                )

    def test_all_segment_source_tables_have_corresponding_view_builders(self) -> None:
        """
        Validates that every YAML source table (except infrastructure tables) has a corresponding view builder
        """
        # Get all source table addresses from YAML configs using shared helper
        segment_event_source_table_addresses = (
            get_segment_frontend_event_source_table_addresses()
        )

        # Collect view builders
        collector = SegmentEventBigQueryViewCollector()
        event_builders = collector.collect_view_builders()

        # Build set of addresses from view builders
        view_builder_addresses = {
            builder.segment_table_sql_source for builder in event_builders
        }

        # Build set of addresses from YAMLs (excluding Segment infrastructure tables
        # that don't represent individual tracked events)
        excluded_table_ids = {
            "identifies",
            "pages",
            "tracks",
            "users",
            "hello",
        }
        yaml_addresses = {
            address
            for address in segment_event_source_table_addresses
            if address.table_id not in excluded_table_ids
            # TODO(#67586): Add view for this segment event
            and address
            != BigQueryAddress(
                dataset_id="jii_frontend_prod_segment_metrics",
                table_id="frontend_cpa_intake_chat_client_address_submitted",
            )
        }

        # Check for YAMLs without view builders
        yaml_without_view = yaml_addresses - view_builder_addresses
        if yaml_without_view:
            raise ValueError(
                f"Found segment event source tables in YAML without corresponding view builders: "
                f"{sorted(addr.to_str() for addr in yaml_without_view)}"
            )

    def test_all_segment_event_tables_have_deduplication_view_exemptions(
        self,
    ) -> None:
        """
        Validates that all segment event source tables (from view builders) have
        corresponding {table_id}_view deduplication tables in the exemption list.
        """
        # Collect view builders to get source tables
        collector = SegmentEventBigQueryViewCollector()
        event_builders = collector.collect_view_builders()

        # Get unique source table addresses from view builders
        source_table_addresses = {
            builder.segment_table_sql_source for builder in event_builders
        }

        # Get the exemption list
        allowed_tables = get_allowed_tables_in_source_table_datasets_with_no_config()

        # Check that each source table has a corresponding _view table in exemptions
        missing_view_exemptions: list[str] = []
        for source_address in sorted(
            source_table_addresses, key=lambda a: (a.dataset_id, a.table_id)
        ):
            view_table_id = f"{source_address.table_id}_view"
            dataset_allowed_tables = allowed_tables.get(
                source_address.dataset_id, set()
            )

            if view_table_id not in dataset_allowed_tables:
                missing_view_exemptions.append(
                    f"{source_address.dataset_id}.{view_table_id}"
                )

        if missing_view_exemptions:
            raise ValueError(
                f"Found segment event source tables without corresponding _view exemptions: "
                f"{missing_view_exemptions}"
            )

    def test_all_collected_builders_have_schemas(self) -> None:
        """Test that all collected segment event builders have non-None schemas."""
        collector = SegmentEventBigQueryViewCollector()
        with local_project_id_override(GCP_PROJECT_STAGING):
            for builder in collector.collect_view_builders():
                view = builder.build()
                self.assertIsNotNone(
                    view.schema,
                    f"Builder for {builder.view_id} has no schema.",
                )

    def test_all_segment_view_builders_have_schemas(self) -> None:
        """Test that ALL segment view builders (event, union, pages) have schemas."""
        with local_project_id_override(GCP_PROJECT_STAGING):
            for builder in get_view_builders_for_views_to_update():
                view = builder.build()
                self.assertIsNotNone(
                    view.schema,
                    f"Builder for {builder.view_id} has no schema.",
                )
