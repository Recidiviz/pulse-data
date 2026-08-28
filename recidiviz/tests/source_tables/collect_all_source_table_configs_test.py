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
from collections import defaultdict

from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import (
    NormalizedStateAgnosticEntitySourceTableLabel,
    StateSpecificSourceTableLabel,
    UnionedStateAgnosticSourceTableLabel,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override

# Datasets that are still split across more than one source table collection,
# grandfathered until the follow-up consolidation work lands. Do not add to this
# list — a new dataset with multiple collections is a bug to fix, not to allow.
_DATASETS_WITH_MULTIPLE_COLLECTIONS = {
    # TODO(OBT-46919): export_archives schemas are split across the
    # externally-managed and YAML-managed packages. Consolidate them into one
    # package (and thus one collection) and drop this entry.
    "export_archives",
    # TODO(OBT-45873): intercom_export schemas are split across the
    # externally-managed and YAML-managed packages. Consolidate them into one
    # package (and thus one collection) and drop this entry.
    "intercom_export",
    # TODO(OBT-46736): collect_duplicative_us_mi_validation_oneoffs builds two
    # non-empty collections sharing this dataset. Merge them into one and drop
    # this entry.
    "us_mi_validation_oneoffs",
}


class CollectAllSourceTableConfigsTest(unittest.TestCase):
    """Test for built source table collections"""

    def test_valid_external_data_configurations(self) -> None:
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with local_project_id_override(project_id):
                source_table_repository = (
                    build_source_table_repository_for_collected_schemata(
                        project_id=project_id
                    )
                )
                for collection in source_table_repository.source_table_collections:
                    for source_table_config in collection.source_tables:
                        source_table_config.validate_source_table_external_data_configuration(
                            collection.update_config
                        )

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
                        isinstance(label, StateSpecificSourceTableLabel)
                        for label in dataset_collection.labels
                    ):
                        # State-specific tables (e.g. raw data, ingest view
                        # results, document store) are not expected to have
                        # `state_code` columns.
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

    def test_one_collection_per_dataset(self) -> None:
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with local_project_id_override(project_id):
                source_table_repository = (
                    build_source_table_repository_for_collected_schemata(
                        project_id=project_id
                    )
                )

                collections_by_dataset: dict[str, int] = defaultdict(int)
                for collection in source_table_repository.source_table_collections:
                    collections_by_dataset[collection.dataset_id] += 1

                datasets_with_multiple_collections = {
                    dataset_id
                    for dataset_id, count in collections_by_dataset.items()
                    if count > 1
                }

                unexpected = (
                    datasets_with_multiple_collections
                    - _DATASETS_WITH_MULTIPLE_COLLECTIONS
                )
                if unexpected:
                    raise ValueError(
                        f"Found datasets backed by more than one source table "
                        f"collection: {sorted(unexpected)}. Every dataset should map "
                        f"to exactly one collection; consolidate the collections for "
                        f"these datasets rather than adding them to "
                        f"_DATASETS_WITH_MULTIPLE_COLLECTIONS."
                    )

                stale = (
                    _DATASETS_WITH_MULTIPLE_COLLECTIONS
                    - datasets_with_multiple_collections
                )
                if stale:
                    raise ValueError(
                        f"Datasets {sorted(stale)} no longer have multiple source "
                        f"table collections. Remove them from "
                        f"_DATASETS_WITH_MULTIPLE_COLLECTIONS."
                    )
