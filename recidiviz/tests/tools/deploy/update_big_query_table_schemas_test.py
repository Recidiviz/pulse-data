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
"""Tests the recidiviz/tools/deploy/update_big_query_table_schemas.py file that issues updates to raw regional
schemas"""

import pytest

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
    collect_raw_data_source_table_collections,
)
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tools.deploy.update_big_query_table_schemas import (
    build_source_table_collection_update_configs,
    update_all_source_table_schemas,
)


def build_bq_snapshot(bq_client: BigQueryClient) -> list[str]:
    """Builds a snapshot of dataset names and their table names to assert against"""
    return [
        f"{dataset.dataset_id}.{table.table_id}"
        for dataset in bq_client.list_datasets()
        for table in bq_client.list_tables(dataset_id=dataset.dataset_id)
        # TODO(#30495): Once we are able to stop loading raw source tables below, we can
        #  delete this filter.
        if "raw_data" not in dataset.dataset_id
    ]


@pytest.mark.uses_bq_emulator
@pytest.mark.usefixtures("snapshottest_snapshot")
class UpdateBigQueryTableSchemasTest(BigQueryEmulatorTestCase):
    """Tests basic functionality of the update_big_query_table_schemas tool"""

    # Deleting our data is actually quite slow. Since this class only has one test case, we can skip it to save time
    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        """Uses the set of raw data tables as input so ingest views can compile"""
        # TODO(#30495): Once we generate ingest view schemas based on types defined in
        #  the ingest mappings YAMLs, we won't need to load the raw data tables in order
        #  to build the ingest view output tables.
        return collect_raw_data_source_table_collections()

    def test_create_dataflow_output_source_tables_matches_snapshot(self) -> None:
        """Tests that the source tables output by the"""
        source_table_repository = SourceTableRepository(
            source_table_collections=get_dataflow_output_source_table_collections()
        )
        update_all_source_table_schemas(
            source_table_repository=source_table_repository,
            update_manager=SourceTableUpdateManager(client=self.bq_client),
            dry_run=False,
        )

        # If this test fails due to a newly added table, re-run it with `pytest --snapshot-update` to resolve:
        # pytest recidiviz/tests/tools/deploy/update_big_query_table_schemas_test.py --snapshot-update
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            build_bq_snapshot(bq_client=self.bq_client),
            name="test_create_dataflow_output_source_tables_matches_snapshot.json",
        )

    def test_no_overlapping_addresses(self) -> None:
        source_table_repository = build_source_table_repository_for_collected_schemata()
        update_configs = build_source_table_collection_update_configs(
            source_table_repository=source_table_repository,
            bq_client=self.bq_client,
        )
        visited_addresses = set()
        duplicate_addresses = set()

        for update_config in update_configs:
            for (
                source_table_config
            ) in update_config.source_table_collection.source_tables:
                address = source_table_config.address
                if address in visited_addresses:
                    duplicate_addresses.add(address.to_str())
                visited_addresses.add(address)

        if duplicate_addresses:
            raise ValueError(
                f"Expected no duplicate addresses across source table collections; found: {duplicate_addresses}"
            )
