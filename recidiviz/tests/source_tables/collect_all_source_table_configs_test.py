# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Test for built source table collections"""
import unittest

from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import (
    IngestViewResultsSourceTableLabel,
    NormalizedStateAgnosticEntitySourceTableLabel,
    RawDataSourceTableLabel,
    StateSpecificSourceTableLabel,
    UnionedStateAgnosticSourceTableLabel,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


class CollectAllSourceTableConfigsTest(unittest.TestCase):
    """Test for built source table collections"""

    def test_state_schema_tables_have_state_code(self) -> None:
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with local_project_id_override(project_id):
                source_table_repository = (
                    build_source_table_repository_for_collected_schemata(
                        project_id=project_id
                    )
                )
                dataset_collections = (
                    source_table_repository.collections_labelled_with(
                        label_type=NormalizedStateAgnosticEntitySourceTableLabel
                    )
                    + source_table_repository.collections_labelled_with(
                        label_type=StateSpecificSourceTableLabel
                    )
                    + source_table_repository.collections_labelled_with(
                        label_type=UnionedStateAgnosticSourceTableLabel
                    )
                )

                for dataset_collection in dataset_collections:
                    if any(
                        isinstance(label, RawDataSourceTableLabel)
                        for label in dataset_collection.labels
                    ):
                        # Raw data tables are state-specific tables and we do not expect
                        # them to have `state_code` columns.
                        continue

                    if any(
                        isinstance(label, IngestViewResultsSourceTableLabel)
                        for label in dataset_collection.labels
                    ):
                        # Ingest view results tables are state-specific tables and we do
                        # not expect them to have `state_code` columns.
                        continue

                    for table in dataset_collection.source_tables:
                        self.assertIn(
                            "state_code",
                            {schema_field.name for schema_field in table.schema_fields},
                            msg=(
                                f"Expected table {table.address} to have state_code "
                                f"column; actual was {table.schema_fields}"
                            ),
                        )

    def test_no_duplicate_addresses_across_collections(self) -> None:
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with local_project_id_override(project_id):
                source_table_repository = (
                    build_source_table_repository_for_collected_schemata(
                        project_id=project_id
                    )
                )
                visited_addresses = set()
                duplicate_addresses = set()

                for collection in source_table_repository.source_table_collections:
                    for source_table_config in collection.source_tables:
                        address = source_table_config.address
                        if address in visited_addresses:
                            duplicate_addresses.add(address.to_str())
                        visited_addresses.add(address)

                if duplicate_addresses:
                    raise ValueError(
                        f"Expected no duplicate addresses across source table "
                        f"collections; found: {duplicate_addresses}"
                    )
