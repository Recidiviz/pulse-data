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
"""Updates ingest mapping YAML files to include column types from BigQuery schema.

This script:
1. Collects all ingest view mapping files for each state
2. Gets corresponding BigQuery table schemas
3. Updates YAML files with BigQuery column types

Usage:
    python -m recidiviz.tools.ingest.one_offs.schema_updater
"""
import os
from typing import Dict, List, Tuple

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class SchemaUpdater:
    """Updates ingest mapping YAML files to include column types from BigQuery schema.

    Attributes:
        bq_client: Client for BigQuery operations
        base_path: Base directory path for ingest mapping files
    """

    def __init__(self) -> None:
        self.bq_client = BigQueryClientImpl(project_id="recidiviz-staging")
        self.base_path = "recidiviz/ingest/direct/regions"

    def _get_ingest_view_collector(
        self, state_code: StateCode
    ) -> IngestViewManifestCollector:
        """Creates a collector for getting ingest views for a state.

        Args:
            state_code: State code (e.g., 'US_TN')

        Returns:
            Collector configured for the given state
        """
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value.lower()
        )
        return IngestViewManifestCollector(
            region=region,
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
        )

    def _construct_yaml_path(self, state_code: str, view_name: str) -> str:
        """Constructs full path to a YAML mapping file.

        Args:
            state_code: Lowercase state code (e.g., 'us_tn')
            view_name: Name of the ingest view

        Returns:
            Full path to YAML file
        """
        file_name = f"{state_code}_{view_name}.yaml"
        return os.path.join(self.base_path, state_code, "ingest_mappings", file_name)

    def get_all_mapping_files(
        self,
    ) -> Dict[StateCode, dict[str, str]]:
        """Gets all YAML mapping files for each state.

        Returns:
            Dictionary mapping state codes to a dictionary of ingest view names to their
            corresponding manifest YAML path.
        """
        mapping_files = {}
        states = get_direct_ingest_states_existing_in_env()

        for state_code in states:
            collector = self._get_ingest_view_collector(state_code)
            mapping_files[state_code] = collector.ingest_view_to_manifest_path
            print(
                f"Found {len(mapping_files[state_code])} mapping files for {state_code}"
            )

        return mapping_files

    def get_bigquery_schema(
        self, table_address: BigQueryAddress
    ) -> List[Tuple[str, str]]:
        """Gets schema information from BigQuery table.

        Args:
            table_address: BigQuery table address

        Returns:
            List of (column_name, type) tuples, excluding internal columns
        """
        table = self.bq_client.get_table(table_address)
        return [
            (field.name, field.field_type)
            for field in table.schema
            if not field.name.startswith("__")
        ]

    def update_yaml_file(self, file_path: str, schema: List[Tuple[str, str]]) -> None:
        """Updates a YAML file with schema type information.

        Args:
            file_path: Path to YAML file
            schema: List of (column_name, type) tuples
        """

        if not schema:
            raise ValueError(f"Expected non-empty schema for [{file_path}]")

        with open(file_path, "r", encoding="utf-8") as f:
            yaml_content = f.readlines()

        new_content = []
        in_input_columns = False
        found_input_columns = False

        # Process each line
        for line in yaml_content:
            if "input_columns:" in line:
                found_input_columns = True
                in_input_columns = True
                new_content.append("input_columns:\n")
                # Add column definitions with types
                for column_name, column_type in schema:
                    new_content.append(f"  {column_name}: {column_type}\n")
            elif not line.strip().startswith("-") and in_input_columns:
                in_input_columns = False
                new_content.append(line)
            elif not in_input_columns:
                new_content.append(line)

        if not found_input_columns:
            print(f"No input_columns section found in {file_path}")
            return

        # Write updated content
        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(new_content)

    def get_table_address(
        self,
        *,
        state_code: StateCode,
        ingest_view_name: str,
    ) -> BigQueryAddress:
        """Gets the BigQuery table address for the given ingest view results table."""
        return BigQueryAddress(
            dataset_id=ingest_view_materialization_results_dataset(
                state_code=state_code
            ),
            table_id=ingest_view_name,
        )

    def run_update(self) -> None:
        """Main function to run the schema update process."""
        mapping_files = self.get_all_mapping_files()

        for state, ingest_view_name_to_file_path in mapping_files.items():
            print(f"Processing state: {state}")

            for ingest_view_name, file_path in ingest_view_name_to_file_path.items():
                print(f"Processing file: {file_path}")
                table_address = self.get_table_address(
                    state_code=state, ingest_view_name=ingest_view_name
                )
                schema = self.get_bigquery_schema(table_address)
                if not schema:
                    raise ValueError(
                        f"Found no columns in the schema for ingest view table "
                        f"{table_address.to_str()}"
                    )
                self.update_yaml_file(file_path, schema)


def main() -> None:
    """Entry point for the schema update script."""
    print("Starting schema update process...")
    updater = SchemaUpdater()
    updater.run_update()
    print("Schema update process completed!")


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        main()
