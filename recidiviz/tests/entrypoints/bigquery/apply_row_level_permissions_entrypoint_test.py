# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for ApplyRowLevelPermissionsEntrypoint."""
import unittest
from unittest.mock import MagicMock, patch

from google.cloud import bigquery, exceptions

from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.bigquery.apply_row_level_permissions_entrypoint import (
    _apply_row_level_permissions_to_all_tables,
    _table_may_need_row_access_policies,
)

_PROJECT_ID = "test-project"

_ENTRYPOINT_MODULE = (
    "recidiviz.entrypoints.bigquery.apply_row_level_permissions_entrypoint"
)

# Use fake state codes so tests don't break when the real restricted states list
# changes.  US_XX is "restricted" and US_YY is "non-restricted".
_FAKE_RESTRICTED_STATES = {StateCode.US_XX: "s-xx-data@recidiviz.org"}


def _make_dataset_list_item(dataset_id: str) -> MagicMock:
    item = MagicMock()
    item.dataset_id = dataset_id
    return item


def _make_table_list_item(
    dataset_id: str,
    table_id: str,
    table_type: str = "TABLE",
) -> bigquery.table.TableListItem:
    return bigquery.table.TableListItem(
        {
            "tableReference": {
                "projectId": _PROJECT_ID,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "type": table_type,
        }
    )


@patch(
    f"{_ENTRYPOINT_MODULE}.RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP",
    _FAKE_RESTRICTED_STATES,
)
class TestTableMayNeedRowAccessPolicies(unittest.TestCase):
    """Tests for the _table_may_need_row_access_policies filter function."""

    def test_view_returns_false(self) -> None:
        item = _make_table_list_item("sessions", "my_view", table_type="VIEW")
        self.assertFalse(_table_may_need_row_access_policies(item))

    def test_external_returns_false(self) -> None:
        item = _make_table_list_item("sessions", "my_ext", table_type="EXTERNAL")
        self.assertFalse(_table_may_need_row_access_policies(item))

    def test_non_restricted_state_dataset_returns_false(self) -> None:
        item = _make_table_list_item("us_yy_raw_data", "some_table")
        self.assertFalse(_table_may_need_row_access_policies(item))

    def test_restricted_state_dataset_returns_true(self) -> None:
        item = _make_table_list_item("us_xx_normalized_state", "state_person")
        self.assertTrue(_table_may_need_row_access_policies(item))

    def test_state_agnostic_dataset_returns_true(self) -> None:
        item = _make_table_list_item("sessions", "compartment_sessions")
        self.assertTrue(_table_may_need_row_access_policies(item))


@patch(
    f"{_ENTRYPOINT_MODULE}.RESTRICTED_ACCESS_STATE_CODE_TO_ACCESS_GROUP",
    _FAKE_RESTRICTED_STATES,
)
@patch(f"{_ENTRYPOINT_MODULE}.DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED", set())
@patch(f"{_ENTRYPOINT_MODULE}.get_source_table_datasets", return_value=set())
@patch(f"{_ENTRYPOINT_MODULE}.metadata")
@patch(f"{_ENTRYPOINT_MODULE}.BigQueryClientImpl")
class TestApplyRowLevelPermissionsEntrypoint(unittest.TestCase):
    """Tests for _apply_row_level_permissions_to_all_tables."""

    def _setup_client(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        dataset_tables: dict[str, list[bigquery.table.TableListItem]],
    ) -> MagicMock:
        """Helper to configure the mock BQ client."""
        mock_metadata.project_id.return_value = _PROJECT_ID
        client = mock_client_cls.return_value

        client.list_datasets.return_value = [
            _make_dataset_list_item(ds) for ds in dataset_tables
        ]

        def mock_list_tables(
            dataset_id: str,
        ) -> list[bigquery.table.TableListItem]:
            if dataset_id in dataset_tables:
                return dataset_tables[dataset_id]
            raise exceptions.NotFound(f"Dataset {dataset_id} not found")

        client.list_tables.side_effect = mock_list_tables

        def mock_get_table(address: MagicMock) -> bigquery.Table:
            table_ref = bigquery.TableReference(
                dataset_ref=bigquery.DatasetReference(
                    project=_PROJECT_ID, dataset_id=address.dataset_id
                ),
                table_id=address.table_id,
            )
            return bigquery.Table(table_ref)

        client.get_table.side_effect = mock_get_table

        return client

    def test_views_and_externals_skipped(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_get_source: MagicMock,
    ) -> None:
        """Views and external tables should not trigger get_table or
        apply_row_level_permissions calls."""
        mock_get_source.return_value = {"my_dataset"}
        dataset_tables = {
            "my_dataset": [
                _make_table_list_item("my_dataset", "my_view", table_type="VIEW"),
                _make_table_list_item(
                    "my_dataset", "my_external", table_type="EXTERNAL"
                ),
                _make_table_list_item("my_dataset", "my_table", table_type="TABLE"),
            ],
        }
        client = self._setup_client(mock_client_cls, mock_metadata, dataset_tables)

        _apply_row_level_permissions_to_all_tables()

        # get_table should only be called for the regular TABLE, not the view or external
        self.assertEqual(client.get_table.call_count, 1)
        called_address = client.get_table.call_args[1]["address"]
        self.assertEqual(called_address.table_id, "my_table")

    def test_non_restricted_state_tables_skipped(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_get_source: MagicMock,
    ) -> None:
        """Tables in non-restricted state datasets should be skipped entirely."""
        mock_get_source.return_value = {"us_yy_raw_data"}
        dataset_tables = {
            "us_yy_raw_data": [
                _make_table_list_item("us_yy_raw_data", "some_table"),
                _make_table_list_item("us_yy_raw_data", "other_table"),
            ],
        }
        client = self._setup_client(mock_client_cls, mock_metadata, dataset_tables)

        _apply_row_level_permissions_to_all_tables()

        client.get_table.assert_not_called()
        client.apply_row_level_permissions.assert_not_called()

    def test_restricted_and_agnostic_tables_processed(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_get_source: MagicMock,
    ) -> None:
        """Tables in restricted state datasets and state-agnostic datasets should
        be processed."""
        mock_get_source.return_value = {"us_xx_state", "sessions"}
        dataset_tables = {
            "us_xx_state": [
                _make_table_list_item("us_xx_state", "state_person"),
            ],
            "sessions": [
                _make_table_list_item("sessions", "compartment_sessions"),
            ],
        }
        client = self._setup_client(mock_client_cls, mock_metadata, dataset_tables)

        _apply_row_level_permissions_to_all_tables()

        self.assertEqual(client.get_table.call_count, 2)
        self.assertEqual(client.apply_row_level_permissions.call_count, 2)

    def test_errors_surfaced_across_all_datasets(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_get_source: MagicMock,
    ) -> None:
        """Errors from tables in different datasets should all be reported."""
        mock_get_source.return_value = {"us_xx_state", "sessions"}
        dataset_tables = {
            "us_xx_state": [
                _make_table_list_item("us_xx_state", "table_a"),
            ],
            "sessions": [
                _make_table_list_item("sessions", "table_b"),
            ],
        }
        client = self._setup_client(mock_client_cls, mock_metadata, dataset_tables)
        client.apply_row_level_permissions.side_effect = ValueError("test error")

        with self.assertRaises(RuntimeError) as ctx:
            _apply_row_level_permissions_to_all_tables()

        error_msg = str(ctx.exception)
        # Both tables from different datasets should appear in the error
        self.assertIn("us_xx_state.table_a", error_msg)
        self.assertIn("sessions.table_b", error_msg)

    def test_not_found_dataset_handled(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_get_source: MagicMock,
    ) -> None:
        """If a dataset is deleted while listing tables, other datasets should
        still be processed."""
        mock_get_source.return_value = {"deleted_dataset", "us_xx_state"}

        client = self._setup_client(
            mock_client_cls,
            mock_metadata,
            # Only us_xx_state has tables; deleted_dataset will raise NotFound
            {
                "deleted_dataset": [],  # placeholder, overridden below
                "us_xx_state": [
                    _make_table_list_item("us_xx_state", "state_person"),
                ],
            },
        )

        def mock_list_tables(
            dataset_id: str,
        ) -> list[bigquery.table.TableListItem]:
            if dataset_id == "deleted_dataset":
                raise exceptions.NotFound("Dataset deleted")
            return [_make_table_list_item("us_xx_state", "state_person")]

        client.list_tables.side_effect = mock_list_tables

        _apply_row_level_permissions_to_all_tables()

        # The table in us_xx_state should still be processed
        self.assertEqual(client.get_table.call_count, 1)
        self.assertEqual(client.apply_row_level_permissions.call_count, 1)
