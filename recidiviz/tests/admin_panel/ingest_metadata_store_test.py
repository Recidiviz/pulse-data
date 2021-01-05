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
"""Implements tests for the IngestMetadataCountsStore class."""
import os
from collections import defaultdict
from typing import Dict, List
from unittest import TestCase, mock

from parameterized import parameterized

from recidiviz.admin_panel.ingest_metadata_store import (
    IngestMetadataCounts,
    IngestMetadataCountsStore,
    IngestMetadataResult,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class TestIngestMetadataStore(TestCase):
    """TestCase for IngestMetadataCountsStore."""

    def setUp(self) -> None:
        self.gcs_factory_patcher = mock.patch('recidiviz.admin_panel.ingest_metadata_store.GcsfsFactory.build')
        fake_gcs = FakeGCSFileSystem()
        fixture_folder = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'fixtures',
        )
        self.table_column_map: Dict[str, List[str]] = defaultdict(list)
        for f in os.listdir(fixture_folder):
            _, table, col = f.split('__')
            self.table_column_map[table].append(col[:-len('.json')])
            path = GcsfsFilePath.from_absolute_path(f'gs://recidiviz-456-ingest-metadata/{f}')
            fake_gcs.test_add_path(path, local_path=os.path.join(fixture_folder, f))

        self.gcs_factory_patcher.start().return_value = fake_gcs
        self.store = IngestMetadataCountsStore(override_project_id='recidiviz-456')
        self.store.recalculate_store()

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()

    def test_object_counts_match(self) -> None:
        self.assertIngestMetadataResultsEqual(
            self.store.fetch_object_counts_by_table(),
            {
                'state_agent': {
                    'US_WW': IngestMetadataCounts.from_json({
                        "total_count": "687179",
                        "placeholder_count": "50180",
                    }),
                    'US_XX': IngestMetadataCounts.from_json({
                        "total_count": "7175527",
                        "placeholder_count": "0",
                    }),
                    'US_YY': IngestMetadataCounts.from_json({
                        "total_count": "274616",
                        "placeholder_count": "179028",
                    }),
                    'US_ZZ': IngestMetadataCounts.from_json({
                        "total_count": "888765",
                        "placeholder_count": "359273",
                    }),
                },
                'state_bond': {},
                'state_charge': {
                    'US_XX': IngestMetadataCounts.from_json({
                        "total_count": "1656434",
                        "placeholder_count": "0",
                    }),
                    'US_YY': IngestMetadataCounts.from_json({
                        "total_count": "386469",
                        "placeholder_count": "123811",
                    }),
                },
            }
        )

    def test_empty_table(self) -> None:
        """Tests that empty tables report empty values but still show up in the main set of results."""
        empty_table = 'state_bond'

        # check that dataset-wide aggregation finds no results for table
        self.assertEqual(0, len(self.store.fetch_object_counts_by_table()[empty_table]))

        # check that table-specific lookup returns no values
        self.assertEqual(0, len(self.store.fetch_table_nonnull_counts_by_column(empty_table)))

        # check that each column reports no values
        for col in self.table_column_map[empty_table]:
            self.assertEqual(0, len(self.store.fetch_column_object_counts_by_value(empty_table, col)),
                             msg=f'Unexpectedly found non-empty results for table {empty_table}, col {col}')

    def test_nonexistent_table(self) -> None:
        """Tests that counts continue to work with a nonexistent table."""
        nonexistent_table = 'nonexistent'

        # check that dataset-wide aggregation finds no results for table
        self.assertTrue(nonexistent_table not in self.store.fetch_object_counts_by_table())

        # check that table-specific lookup returns no values
        self.assertEqual(0, len(self.store.fetch_table_nonnull_counts_by_column(nonexistent_table)))

    def test_count_primary_keys(self) -> None:
        """Tests that primary key values should only report a single type, non-null."""
        results = self.store.fetch_column_object_counts_by_value('state_agent', 'agent_id')
        self.assertEqual(
            1,
            len(results),
            msg='Primary key field should not report more than one observed type (NOT_NULL only).')

    @parameterized.expand([
        ('state_agent', ['US_WW', 'US_XX', 'US_YY', 'US_ZZ']),
        ('state_charge', ['US_XX', 'US_YY']),
    ])
    def test_state_consistency(self, table: str, states: List[str]) -> None:
        """Tests that the same states should be present across all results for the table."""

        object_counts = self.store.fetch_object_counts_by_table()[table]
        self.assertEqual(
            len(states),
            len(object_counts),
            msg='Not all states present in call to `fetch_object_counts_by_table`.')

        for state in states:
            self.assertTrue(state in object_counts,
                            msg=f'State {state} not found in `fetch_object_counts_by_table` results.')

        nonnull_counts = self.store.fetch_table_nonnull_counts_by_column(table)
        for col, results in nonnull_counts.items():
            # We check >= since there may be columns that are always NULL for some states.
            self.assertGreaterEqual(
                len(states),
                len(results),
                msg=f'More states than expected in call to `fetch_table_nonnull_counts_by_column` for column {col}.')

            value_found_for_state = {state: False for state in states}

            obj_counts_by_values = self.store.fetch_column_object_counts_by_value(table, col)
            for value_counts in obj_counts_by_values.values():
                for state in value_found_for_state:
                    if state in value_counts:
                        value_found_for_state[state] = True

            for state, found in value_found_for_state.items():
                self.assertTrue(found, msg=f'Did not find value for state {state} in column {col}')

    def assertIngestMetadataResultsEqual(self, r1: IngestMetadataResult, r2: IngestMetadataResult) -> None:
        self.assertEqual(len(r1), len(r2), msg='The two results have differing numbers of values.')

        for key, state_map1 in r1.items():
            state_map2 = r2[key]

            self.assertEqual(
                len(state_map1),
                len(state_map2),
                msg=f'The two results have differing numbers of states for value {key}')

            for state, count1 in state_map1.items():
                count2 = state_map2[state]

                self.assertEqual(
                    count1.total_count,
                    count2.total_count,
                    msg=f"Total counts for state {state} and value {key} do not match.")

                self.assertEqual(
                    count1.placeholder_count,
                    count2.placeholder_count,
                    msg=f"Placeholder counts for state {state} and value {key} do not match.")
