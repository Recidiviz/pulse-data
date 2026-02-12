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
"""Tests for validate_clean_source_table_datasets"""
import unittest
from collections.abc import Iterator
from unittest.mock import MagicMock, create_autospec, patch

from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_cleanup_validation import (
    validate_clean_source_table_datasets,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.untracked_source_table_exemptions import (
    ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG,
)


def _make_source_table_config(dataset_id: str, table_id: str) -> SourceTableConfig:
    return SourceTableConfig(
        address=BigQueryAddress(dataset_id=dataset_id, table_id=table_id),
        description="test table",
        schema_fields=[SchemaField("id", "INTEGER")],
    )


def _make_collection(dataset_id: str, table_ids: list[str]) -> SourceTableCollection:
    configs = {
        BigQueryAddress(dataset_id=dataset_id, table_id=tid): _make_source_table_config(
            dataset_id, tid
        )
        for tid in table_ids
    }
    return SourceTableCollection(
        dataset_id=dataset_id,
        description=f"Test collection for {dataset_id}",
        update_config=SourceTableCollectionUpdateConfig.externally_managed(),
        source_tables_by_address=configs,
    )


def _make_table_list_item(table_id: str) -> MagicMock:
    """Creates a mock TableListItem with a given table_id."""
    item = MagicMock()
    item.table_id = table_id
    return item


class TestValidateCleanSourceTableDatasets(unittest.TestCase):
    """Tests for validate_clean_source_table_datasets"""

    def setUp(self) -> None:
        self.mock_bq_client = create_autospec(BigQueryClient)

    def test_passes_when_bq_matches_configs(self) -> None:
        """Validation passes when BQ tables exactly match source table configs."""
        repo = SourceTableRepository(
            source_table_collections=[
                _make_collection("my_dataset", ["table_a", "table_b"]),
            ]
        )
        self.mock_bq_client.list_tables.return_value = iter(
            [_make_table_list_item("table_a"), _make_table_list_item("table_b")]
        )

        # Should not raise
        validate_clean_source_table_datasets(
            bq_client=self.mock_bq_client,
            source_table_repository=repo,
        )

    def test_raises_on_unexpected_tables(self) -> None:
        """Validation raises when BQ has tables not in source configs."""
        repo = SourceTableRepository(
            source_table_collections=[
                _make_collection("my_dataset", ["table_a"]),
            ]
        )
        self.mock_bq_client.list_tables.return_value = iter(
            [_make_table_list_item("table_a"), _make_table_list_item("rogue_table")]
        )

        with self.assertRaises(ValueError) as ctx:
            validate_clean_source_table_datasets(
                bq_client=self.mock_bq_client,
                source_table_repository=repo,
            )
        self.assertIn("unexpected tables", str(ctx.exception))
        self.assertIn("rogue_table", str(ctx.exception))

    def test_raises_on_missing_tables(self) -> None:
        """Validation raises when source configs expect tables not in BQ."""
        repo = SourceTableRepository(
            source_table_collections=[
                _make_collection("my_dataset", ["table_a", "table_b"]),
            ]
        )
        self.mock_bq_client.list_tables.return_value = iter(
            [_make_table_list_item("table_a")]
        )

        with self.assertRaises(ValueError) as ctx:
            validate_clean_source_table_datasets(
                bq_client=self.mock_bq_client,
                source_table_repository=repo,
            )
        self.assertIn("missing expected tables", str(ctx.exception))
        self.assertIn("table_b", str(ctx.exception))

    def test_raises_on_both_unexpected_and_missing(self) -> None:
        """Validation reports both unexpected and missing tables."""
        repo = SourceTableRepository(
            source_table_collections=[
                _make_collection("my_dataset", ["expected_table"]),
            ]
        )
        self.mock_bq_client.list_tables.return_value = iter(
            [_make_table_list_item("unexpected_table")]
        )

        with self.assertRaises(ValueError) as ctx:
            validate_clean_source_table_datasets(
                bq_client=self.mock_bq_client,
                source_table_repository=repo,
            )
        error_msg = str(ctx.exception)
        self.assertIn("unexpected tables", error_msg)
        self.assertIn("unexpected_table", error_msg)
        self.assertIn("missing expected tables", error_msg)
        self.assertIn("expected_table", error_msg)

    @patch(
        "recidiviz.source_tables.source_table_cleanup_validation"
        ".ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG",
        {"my_dataset": {"legacy_table"}},
    )
    def test_legacy_tables_are_allowed(self) -> None:
        """Legacy tables in the allowlist don't trigger unexpected table errors."""
        repo = SourceTableRepository(
            source_table_collections=[
                _make_collection("my_dataset", ["table_a"]),
            ]
        )
        self.mock_bq_client.list_tables.return_value = iter(
            [_make_table_list_item("table_a"), _make_table_list_item("legacy_table")]
        )

        # Should not raise
        validate_clean_source_table_datasets(
            bq_client=self.mock_bq_client,
            source_table_repository=repo,
        )

    def test_skips_empty_collections(self) -> None:
        """Collections with no expected tables (filtered by deployed_projects) are skipped."""
        empty_collection = SourceTableCollection(
            dataset_id="empty_dataset",
            description="Empty collection",
            update_config=SourceTableCollectionUpdateConfig.externally_managed(),
            source_tables_by_address={},
        )
        repo = SourceTableRepository(source_table_collections=[empty_collection])

        # list_tables should never be called for an empty collection
        validate_clean_source_table_datasets(
            bq_client=self.mock_bq_client,
            source_table_repository=repo,
        )
        self.mock_bq_client.list_tables.assert_not_called()

    def test_multiple_datasets(self) -> None:
        """Validation checks multiple datasets independently."""
        repo = SourceTableRepository(
            source_table_collections=[
                _make_collection("dataset_a", ["t1", "t2"]),
                _make_collection("dataset_b", ["t3"]),
            ]
        )

        def list_tables_side_effect(dataset_id: str) -> Iterator[MagicMock]:
            if dataset_id == "dataset_a":
                return iter([_make_table_list_item("t1"), _make_table_list_item("t2")])
            if dataset_id == "dataset_b":
                return iter([_make_table_list_item("t3")])
            return iter([])

        self.mock_bq_client.list_tables.side_effect = list_tables_side_effect

        # Should not raise
        validate_clean_source_table_datasets(
            bq_client=self.mock_bq_client,
            source_table_repository=repo,
        )

    def test_multiple_datasets_with_errors_in_one(self) -> None:
        """Errors in one dataset don't prevent checking other datasets."""
        repo = SourceTableRepository(
            source_table_collections=[
                _make_collection("good_dataset", ["t1"]),
                _make_collection("bad_dataset", ["t2"]),
            ]
        )

        def list_tables_side_effect(dataset_id: str) -> Iterator[MagicMock]:
            if dataset_id == "good_dataset":
                return iter([_make_table_list_item("t1")])
            if dataset_id == "bad_dataset":
                return iter(
                    [_make_table_list_item("t2"), _make_table_list_item("surprise")]
                )
            return iter([])

        self.mock_bq_client.list_tables.side_effect = list_tables_side_effect

        with self.assertRaises(ValueError) as ctx:
            validate_clean_source_table_datasets(
                bq_client=self.mock_bq_client,
                source_table_repository=repo,
            )
        error_msg = str(ctx.exception)
        self.assertIn("bad_dataset", error_msg)
        self.assertIn("surprise", error_msg)
        self.assertNotIn("good_dataset", error_msg)


class TestExemptionListNoOverlapWithConfigs(unittest.TestCase):
    """Tests that tables in the no-config exemption list don't also have YAML configs."""

    def test_no_overlap_between_exemptions_and_yaml_configs(self) -> None:
        """Tables with YAML configs should be removed from
        ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG.
        """
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=None
        )
        configured_source_table_addresses: set[BigQueryAddress] = set(
            source_table_repository.source_tables
        )

        exempted_table_addresses: set[BigQueryAddress] = set()
        for dataset_id, exempted_tables in sorted(
            ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG.items()
        ):
            exempted_table_addresses.update(
                {
                    BigQueryAddress(dataset_id=dataset_id, table_id=table_id)
                    for table_id in exempted_tables
                }
            )

        overlapping = configured_source_table_addresses & exempted_table_addresses

        self.assertEqual(
            overlapping,
            set(),
            "The following tables have YAML configs but are still listed in "
            "ALLOWED_TABLES_IN_SOURCE_TABLE_DATASETS_WITH_NO_CONFIG in "
            "untracked_source_table_exemptions.py. Please remove them from "
            f"the exemption list:{BigQueryAddress.addresses_to_str(overlapping, indent_level=2)}",
        )
