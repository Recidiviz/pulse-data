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
"""Tests for ApplyDatasetProtectionTagsEntrypoint."""
import argparse
import unittest
from typing import Optional
from unittest.mock import MagicMock, patch

from recidiviz.entrypoints.bigquery.apply_dataset_protection_tags_entrypoint import (
    PROTECTION_TAG_KEY,
    PROTECTION_TAG_VALUE,
    ApplyDatasetProtectionTagsEntrypoint,
    apply_protection_tags,
    datasets_to_protect,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollectionUpdateConfig,
)

_ENTRYPOINT_MODULE = (
    "recidiviz.entrypoints.bigquery.apply_dataset_protection_tags_entrypoint"
)
_PROJECT_ID = "test-project"


def _collection(
    dataset_id: str, update_config: SourceTableCollectionUpdateConfig
) -> MagicMock:
    collection = MagicMock()
    collection.dataset_id = dataset_id
    collection.update_config = update_config
    return collection


def _repository(collections: list[MagicMock]) -> MagicMock:
    repository = MagicMock()
    repository.source_table_collections = collections
    return repository


def _dataset_list_item(dataset_id: str) -> MagicMock:
    item = MagicMock()
    item.dataset_id = dataset_id
    return item


class _FakeDataset:
    """Minimal stand-in for a bigquery.Dataset with a mutable resource_tags field."""

    def __init__(self, resource_tags: Optional[dict[str, str]] = None) -> None:
        self.resource_tags = resource_tags


@patch(f"{_ENTRYPOINT_MODULE}.build_source_table_repository_for_collected_schemata")
class DatasetsToProtectTest(unittest.TestCase):
    """Tests the protect-tier selection (everything except .regenerable())."""

    def test_excludes_regenerable_keeps_protected_external_and_allowlist(
        self, mock_build: MagicMock
    ) -> None:
        mock_build.return_value = _repository(
            [
                _collection(
                    "us_xx_raw_data", SourceTableCollectionUpdateConfig.protected()
                ),
                _collection(
                    "segment_data",
                    SourceTableCollectionUpdateConfig.externally_managed(),
                ),
                _collection(
                    "dataflow_metrics", SourceTableCollectionUpdateConfig.regenerable()
                ),
            ]
        )

        with patch(
            f"{_ENTRYPOINT_MODULE}.INFRA_PROTECT_ALLOWLIST", {"cloud_sql_to_bq_refresh"}
        ):
            result = datasets_to_protect(_PROJECT_ID)

        self.assertEqual(
            {"us_xx_raw_data", "segment_data", "cloud_sql_to_bq_refresh"}, result
        )
        self.assertNotIn("dataflow_metrics", result)

    def test_dataset_protected_if_any_collection_is_non_regenerable(
        self, mock_build: MagicMock
    ) -> None:
        # Same dataset appears in a regenerable AND a protected collection -> protect it.
        mock_build.return_value = _repository(
            [
                _collection("mixed", SourceTableCollectionUpdateConfig.regenerable()),
                _collection("mixed", SourceTableCollectionUpdateConfig.protected()),
            ]
        )

        with patch(f"{_ENTRYPOINT_MODULE}.INFRA_PROTECT_ALLOWLIST", set()):
            self.assertEqual({"mixed"}, datasets_to_protect(_PROJECT_ID))


@patch(f"{_ENTRYPOINT_MODULE}.INFRA_PROTECT_ALLOWLIST", set())
@patch(f"{_ENTRYPOINT_MODULE}.build_source_table_repository_for_collected_schemata")
@patch(f"{_ENTRYPOINT_MODULE}.metadata")
@patch(f"{_ENTRYPOINT_MODULE}.BigQueryClientImpl")
class ApplyProtectionTagsTest(unittest.TestCase):
    """Tests the reconcile behavior: add-only, idempotent, dry-run, drift."""

    def _wire(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_build: MagicMock,
        *,
        protect: set[str],
        live: dict[str, Optional[dict[str, str]]],
    ) -> tuple[MagicMock, dict[str, _FakeDataset]]:
        """Configures the mocks. `protect` = protect-tier dataset ids; `live` = existing
        datasets mapped to their current resource_tags (None = untagged)."""
        mock_metadata.project_id.return_value = _PROJECT_ID
        mock_build.return_value = _repository(
            [
                _collection(d, SourceTableCollectionUpdateConfig.protected())
                for d in protect
            ]
        )
        datasets = {d: _FakeDataset(resource_tags=tags) for d, tags in live.items()}
        client = MagicMock()
        client.list_datasets.return_value = [_dataset_list_item(d) for d in live]
        client.get_dataset.side_effect = lambda dataset_id: datasets[dataset_id]
        mock_client_cls.return_value = client
        return client, datasets

    def test_dry_run_makes_no_writes(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_build: MagicMock,
    ) -> None:
        client, _ = self._wire(
            mock_client_cls,
            mock_metadata,
            mock_build,
            protect={"a", "b"},
            live={"a": None, "b": None, "other": None},
        )
        apply_protection_tags(dry_run=True)
        client.client.update_dataset.assert_not_called()

    def test_tags_missing_skips_present_and_ignores_non_protect(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_build: MagicMock,
    ) -> None:
        client, datasets = self._wire(
            mock_client_cls,
            mock_metadata,
            mock_build,
            protect={"a", "b"},
            live={
                "a": None,  # protect + untagged -> gets tagged
                "b": {
                    PROTECTION_TAG_KEY: PROTECTION_TAG_VALUE
                },  # already tagged -> skipped
                "other": None,  # not in protect set -> untouched
            },
        )
        apply_protection_tags(dry_run=False)

        updated = [call.args[0] for call in client.client.update_dataset.call_args_list]
        self.assertEqual([datasets["a"]], updated)
        self.assertEqual(
            {PROTECTION_TAG_KEY: PROTECTION_TAG_VALUE}, datasets["a"].resource_tags
        )
        self.assertIsNone(datasets["other"].resource_tags)

    def test_add_only_merges_with_existing_tags(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_build: MagicMock,
    ) -> None:
        _, datasets = self._wire(
            mock_client_cls,
            mock_metadata,
            mock_build,
            protect={"a"},
            live={"a": {"some-other-tag": "keep-me"}},
        )
        apply_protection_tags(dry_run=False)
        self.assertEqual(
            {"some-other-tag": "keep-me", PROTECTION_TAG_KEY: PROTECTION_TAG_VALUE},
            datasets["a"].resource_tags,
        )

    def test_skips_registry_datasets_not_yet_created(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_build: MagicMock,
    ) -> None:
        client, _ = self._wire(
            mock_client_cls,
            mock_metadata,
            mock_build,
            protect={"a", "not_created_yet"},
            live={"a": None},
        )
        apply_protection_tags(dry_run=False)
        # get_dataset is only ever called for datasets that actually exist.
        for call in client.get_dataset.call_args_list:
            self.assertNotEqual("not_created_yet", call.args[0])

    def test_drift_is_logged_never_removed(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_build: MagicMock,
    ) -> None:
        client, datasets = self._wire(
            mock_client_cls,
            mock_metadata,
            mock_build,
            protect={"a"},
            live={"a": None, "drifted": {PROTECTION_TAG_KEY: PROTECTION_TAG_VALUE}},
        )
        with self.assertLogs(level="WARNING") as logs:
            apply_protection_tags(dry_run=False)

        self.assertTrue(
            any(
                "PROTECTION DRIFT" in line and "drifted" in line for line in logs.output
            )
        )
        # The drifted dataset keeps its tag -- the only write is adding the tag to "a".
        self.assertEqual(
            {PROTECTION_TAG_KEY: PROTECTION_TAG_VALUE},
            datasets["drifted"].resource_tags,
        )
        self.assertEqual(
            [datasets["a"]],
            [call.args[0] for call in client.client.update_dataset.call_args_list],
        )

    def test_tag_failure_is_non_fatal(
        self,
        mock_client_cls: MagicMock,
        mock_metadata: MagicMock,
        mock_build: MagicMock,
    ) -> None:
        client, datasets = self._wire(
            mock_client_cls,
            mock_metadata,
            mock_build,
            protect={"boom", "ok"},
            live={"boom": None, "ok": None},
        )

        # Tagging "boom" raises (e.g. a missing permission); "ok" still succeeds.
        def _update(dataset: object, _fields: list[str]) -> None:
            if dataset is datasets["boom"]:
                raise RuntimeError("permission denied")

        client.client.update_dataset.side_effect = _update

        with self.assertLogs(level="WARNING") as logs:
            apply_protection_tags(dry_run=False)  # must not raise

        self.assertTrue(any("boom" in line for line in logs.output))
        self.assertEqual(
            {PROTECTION_TAG_KEY: PROTECTION_TAG_VALUE}, datasets["ok"].resource_tags
        )


class EntrypointWiringTest(unittest.TestCase):
    """Tests the argparse + run_entrypoint plumbing."""

    def test_parser_dry_run_flag(self) -> None:
        parser = ApplyDatasetProtectionTagsEntrypoint.get_parser()
        self.assertTrue(parser.parse_args(["--dry-run"]).dry_run)
        self.assertFalse(parser.parse_args([]).dry_run)

    @patch(f"{_ENTRYPOINT_MODULE}.apply_protection_tags")
    def test_run_entrypoint_delegates(self, mock_apply: MagicMock) -> None:
        ApplyDatasetProtectionTagsEntrypoint.run_entrypoint(
            args=argparse.Namespace(dry_run=True)
        )
        mock_apply.assert_called_once_with(dry_run=True)
