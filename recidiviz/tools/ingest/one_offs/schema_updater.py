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
3. Updates YAML files with BigQuery column types in parallel

Usage:
    python -m recidiviz.tools.ingest.one_offs.schema_updater
"""
import multiprocessing as mp
from dataclasses import dataclass
from typing import List, Optional, Tuple

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


@dataclass
class UpdateTask:
    """Data container for a single file update task."""

    state_code: StateCode
    ingest_view_name: str
    file_path: str


class SchemaUpdaterWorker:
    """Worker class that handles individual update tasks."""

    def __init__(self) -> None:
        self.bq_client: Optional[BigQueryClientImpl] = None

    def ensure_client(self) -> None:
        """Ensures BigQuery client exists, creating it if necessary."""
        if self.bq_client is None:
            self.bq_client = BigQueryClientImpl(project_id="recidiviz-staging")

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

    def get_bigquery_schema(
        self, table_address: BigQueryAddress
    ) -> List[Tuple[str, str]]:
        """Gets schema information from BigQuery table."""
        self.ensure_client()
        assert self.bq_client is not None

        table = self.bq_client.get_table(table_address)
        return [
            (field.name, field.field_type)
            for field in table.schema
            if not field.name.startswith("__")
        ]

    def update_yaml_file(self, file_path: str, schema: List[Tuple[str, str]]) -> None:
        """Updates a YAML file with schema type information."""
        if not schema:
            raise ValueError(f"Expected non-empty schema for [{file_path}]")

        with open(file_path, "r", encoding="utf-8") as f:
            yaml_content = f.readlines()

        new_content = []
        in_input_columns = False
        found_input_columns = False

        for line in yaml_content:
            if "input_columns:" in line:
                found_input_columns = True
                in_input_columns = True
                new_content.append("input_columns:\n")
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

        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(new_content)

    def process_task(self, task: UpdateTask) -> None:
        """Process a single update task."""
        print(f"Processing file: {task.file_path}")
        table_address = self.get_table_address(
            state_code=task.state_code, ingest_view_name=task.ingest_view_name
        )
        schema = self.get_bigquery_schema(table_address)
        if not schema:
            raise ValueError(
                f"Found no columns in the schema for ingest view table "
                f"{table_address.to_str()}"
            )
        self.update_yaml_file(task.file_path, schema)


class ParallelSchemaUpdater:
    """Updates ingest mapping YAML files to include column types from BigQuery schema."""

    def __init__(
        self, num_processes: int = 6, expected_num_ingest_views: int = 218
    ) -> None:
        """Initialize the updater with the specified number of processes."""
        self.base_path = "recidiviz/ingest/direct/regions"
        self.expected_num_ingest_views = expected_num_ingest_views
        self.num_processes = num_processes

    def _get_ingest_view_collector(
        self, state_code: StateCode
    ) -> IngestViewManifestCollector:
        """Creates a collector for getting ingest views for a state."""
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=state_code.value.lower()
        )
        return IngestViewManifestCollector(
            region=region,
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
        )

    def get_all_mapping_files(self) -> List[UpdateTask]:
        """Gets all YAML mapping files for each state."""
        tasks = []
        states = get_direct_ingest_states_existing_in_env()

        for state_code in states:
            collector = self._get_ingest_view_collector(state_code)
            for (
                ingest_view_name,
                file_path,
            ) in collector.ingest_view_to_manifest_path.items():
                tasks.append(UpdateTask(state_code, ingest_view_name, file_path))

            print(
                f"Found {len(collector.ingest_view_to_manifest_path)} mapping files for {state_code}"
            )

        return tasks

    def run_update(self) -> None:
        """Main function to run the parallel schema update process."""
        tasks = self.get_all_mapping_files()

        assert len(tasks) == self.expected_num_ingest_views, (
            f"Got {len(tasks)} tasks but expected {self.expected_num_ingest_views} ingest views. "
            f"This might indicate missing or extra mapping files."
        )

        worker = SchemaUpdaterWorker()

        # Create a pool of worker processes and map tasks to it
        with mp.Pool(processes=self.num_processes) as pool:
            pool.map(worker.process_task, tasks)


def main() -> None:
    """Entry point for the parallel schema update script."""
    print("Starting parallel schema update process...")

    updater = ParallelSchemaUpdater(num_processes=6, expected_num_ingest_views=218)
    updater.run_update()
    print("Schema update process completed!")


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        main()
