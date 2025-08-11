# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
# TODO(#26022): Remove placeholder references in admin panel state table views
"""Implements tests for the DatasetMetadataCountsStore class."""
import os
from collections import defaultdict
from typing import Dict, List
from unittest import TestCase, mock
from unittest.mock import patch

from fakeredis import FakeRedis
from parameterized import parameterized

from recidiviz.admin_panel.dataset_metadata_store import (
    DatasetMetadataCounts,
    DatasetMetadataCountsStore,
    DatasetMetadataResult,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class TestDatasetMetadataStore(TestCase):
    """TestCase for DatasetMetadataStore."""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.project_id_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

        self.redis_patcher = patch(
            "recidiviz.admin_panel.admin_panel_store.get_admin_panel_redis"
        )
        self.mock_redis_patcher = self.redis_patcher.start()
        self.mock_redis_patcher.return_value = FakeRedis()

        self.gcs_factory_patcher = mock.patch(
            "recidiviz.admin_panel.dataset_metadata_store.GcsfsFactory.build"
        )

        fake_gcs = FakeGCSFileSystem()

        fixture_folder = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "fixtures",
        )
        self.table_column_map: Dict[str, List[str]] = defaultdict(list)
        for f in os.listdir(fixture_folder):
            _, table, col = f.split("__")
            self.table_column_map[table].append(col[: -len(".json")])
            path = GcsfsFilePath.from_absolute_path(
                f"gs://recidiviz-456-ingest-metadata/{f}"
            )
            fake_gcs.test_add_path(path, local_path=os.path.join(fixture_folder, f))

        self.gcs_factory_patcher.start().return_value = fake_gcs
        self.store = DatasetMetadataCountsStore(
            dataset_nickname="ingest",
            metadata_file_prefix="ingest_state_metadata",
        )
        self.store.hydrate_cache()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.gcs_factory_patcher.stop()
        self.mock_redis_patcher.stop()

    def test_object_counts_match(self) -> None:
        self.assertIngestMetadataResultsEqual(
            {
                "state_staff": {
                    "US_WW": DatasetMetadataCounts.from_json(
                        {
                            "total_count": "687179",
                            "placeholder_count": "50180",
                        }
                    ),
                    "US_XX": DatasetMetadataCounts.from_json(
                        {
                            "total_count": "7175527",
                            "placeholder_count": "0",
                        }
                    ),
                    "US_YY": DatasetMetadataCounts.from_json(
                        {
                            "total_count": "274616",
                            "placeholder_count": "179028",
                        }
                    ),
                    "US_ZZ": DatasetMetadataCounts.from_json(
                        {
                            "total_count": "876944",
                            "placeholder_count": "359273",
                        }
                    ),
                },
                "state_charge": {
                    "US_XX": DatasetMetadataCounts.from_json(
                        {
                            "total_count": "1656434",
                            "placeholder_count": "0",
                        }
                    ),
                    "US_YY": DatasetMetadataCounts.from_json(
                        {
                            "total_count": "386469",
                            "placeholder_count": "123811",
                        }
                    ),
                },
            },
            self.store.fetch_object_counts_by_table(),
        )

    def test_empty_table(self) -> None:
        """Tests that empty tables report empty values but still show up in the main set of results."""
        empty_table = "state_incarceration_incident_outcome"

        # check that dataset-wide aggregation finds no results for table
        self.assertEqual(0, len(self.store.fetch_object_counts_by_table()[empty_table]))

        # check that each column reports no values
        for col in self.table_column_map[empty_table]:
            self.assertEqual(
                0,
                len(self.store.fetch_column_object_counts_by_value(empty_table, col)),
                msg=f"Unexpectedly found non-empty results for table {empty_table}, col {col}",
            )

    def test_nonexistent_table(self) -> None:
        """Tests that counts continue to work with a nonexistent table."""
        nonexistent_table = "nonexistent"

        # check that dataset-wide aggregation finds no results for table
        self.assertTrue(
            nonexistent_table not in self.store.fetch_object_counts_by_table()
        )

    def test_count_primary_keys(self) -> None:
        """Tests that primary key values should only report a single type, non-null."""
        results = self.store.fetch_column_object_counts_by_value(
            "state_staff", "staff_id"
        )
        self.assertEqual(
            1,
            len(results),
            msg="Primary key field should not report more than one observed type (NOT_NULL only).",
        )

    @parameterized.expand(
        [
            ("state_staff", ["US_WW", "US_XX", "US_YY", "US_ZZ"]),
            ("state_charge", ["US_XX", "US_YY"]),
        ]
    )
    def test_state_consistency(self, table: str, states: List[str]) -> None:
        """Tests that the same states should be present across all results for the table."""
        store_data = self.store.fetch_data()
        object_counts = self.store.fetch_object_counts_by_table()[table]
        self.assertEqual(
            len(states),
            len(object_counts),
            msg="Not all states present in call to `fetch_object_counts_by_table`.",
        )

        for state in states:
            self.assertTrue(
                state in object_counts,
                msg=f"State {state} not found in `fetch_object_counts_by_table` results.",
            )

        nonnull_counts = self.store.fetch_table_nonnull_counts_by_column(
            table_data=store_data[table]
        )
        for col, results in nonnull_counts.items():
            # We check >= since there may be columns that are always NULL for some states.
            self.assertGreaterEqual(
                len(states),
                len(results),
                msg=f"More states than expected in call to `fetch_table_nonnull_counts_by_column` for column {col}.",
            )

            value_found_for_state = {state: False for state in states}

            obj_counts_by_values = self.store.fetch_column_object_counts_by_value(
                table, col
            )
            for value_counts in obj_counts_by_values.values():
                for state in value_found_for_state:
                    if state in value_counts:
                        value_found_for_state[state] = True

            for state, found in value_found_for_state.items():
                self.assertTrue(
                    found, msg=f"Did not find value for state {state} in column {col}"
                )

    def assertIngestMetadataResultsEqual(
        self, r1: DatasetMetadataResult, r2: DatasetMetadataResult
    ) -> None:
        self.assertEqual(
            len(r1), len(r2), msg="The two results have differing numbers of values."
        )

        for key, state_map1 in r1.items():
            state_map2 = r2[key]

            self.assertEqual(
                len(state_map1),
                len(state_map2),
                msg=f"The two results have differing numbers of states for value {key}",
            )

            for state, count1 in state_map1.items():
                count2 = state_map2[state]

                self.assertEqual(
                    count1.total_count,
                    count2.total_count,
                    msg=f"Total counts for state {state} and value {key} do not match.",
                )

                self.assertEqual(
                    count1.placeholder_count,
                    count2.placeholder_count,
                    msg=f"Placeholder counts for state {state} and value {key} do not match.",
                )
