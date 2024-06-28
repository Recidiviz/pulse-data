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

from recidiviz.pipelines.pipeline_names import NORMALIZATION_PIPELINE_NAME
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    NormalizedStateAgnosticEntitySourceTableLabel,
)


class CollectAllSourceTableConfigsTest(unittest.TestCase):
    """Test for built source table collections"""

    def test_normalized_state_tables_have_state_code(self) -> None:
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=None
        )
        normalization_datasets = (
            source_table_repository.get_collections(
                labels=[
                    NormalizedStateAgnosticEntitySourceTableLabel(
                        source_is_normalization_pipeline=True
                    )
                ]
            )
            + source_table_repository.get_collections(
                labels=[
                    NormalizedStateAgnosticEntitySourceTableLabel(
                        source_is_normalization_pipeline=False
                    )
                ]
            )
            + source_table_repository.get_collections(
                labels=[
                    DataflowPipelineSourceTableLabel(
                        pipeline_name=NORMALIZATION_PIPELINE_NAME
                    )
                ]
            )
        )

        for normalization_dataset in normalization_datasets:
            for table in normalization_dataset.source_tables:
                self.assertIn(
                    "state_code",
                    {schema_field.name for schema_field in table.schema_fields},
                    msg=f"Expected table {table.address} to have state_code column; actual was {table.schema_fields}",
                )

    def test_no_duplicate_addresses_across_collections(self) -> None:
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=None
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
                f"Expected no duplicate addresses across source table collections; found: {duplicate_addresses}"
            )
