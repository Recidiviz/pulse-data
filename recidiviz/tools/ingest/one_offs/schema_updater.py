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
"""Updates ingest mapping YAML files to include column types from BigQuery schema or assigns STRING type for test files.

!!! IMPORTANT !!!

Before running this script, you MUST manually update the type annotation for 'input_columns' 
in "recidiviz/ingest/direct/ingest_mappings/ingest_view_manifest_compiler.py"
to expect Union[List[str], Dict[str, str]].

Add appropriate isinstance() checks in your validation logic to handle both formats during migration.

The script converts the format from list to dict but does not update type checking.

This script handles three types of YAML mappings:
1. Production state files - Updates with actual BigQuery schema types
2. Test fixture files - Assigns STRING type to all columns
3. Fake region files - Assigns STRING type to all columns

The script updates input_columns format from:
    input_columns:
      - StaffID
      - LastName

To:
    input_columns:
      StaffID: STRING 
      LastName: STRING

Usage:
    python -m recidiviz.tools.ingest.one_offs.schema_updater \
        --path-type <production|test|fake> \
        --expected-yaml-files <count> \
        [--num-processes <num>]
        
Example:
    # Update production state files
    python -m recidiviz.tools.ingest.one_offs.schema_updater \
        --path-type production \
        --expected-yaml-files 218

    # Update test fixtures  
    python -m recidiviz.tools.ingest.one_offs.schema_updater \
        --path-type test \
        --expected-yaml-files 25

    # Update fake region ingest mappings
    python -m recidiviz.tools.ingest.one_offs.schema_updater \
        --path-type fake \
        --expected-yaml-files 25
"""

import argparse
import glob
import multiprocessing as mp
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Tuple

import yaml

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

    state_code: Optional[StateCode] | str
    ingest_view_name: str
    file_path: str
    is_fake_region: bool = False


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
        if state_code is None:
            raise ValueError("State code must be provided for non-test updates.")
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
        """Updates a YAML file with schema type information only if it uses the old format."""
        if not schema:
            raise ValueError(f"Expected non-empty schema for [{file_path}]")

        with open(file_path, "r", encoding="utf-8") as f:
            yaml_content = f.readlines()

        # check if we need to update by looking for the old format
        needs_update = False
        in_input_columns = False
        for line_num, line in enumerate(yaml_content):
            stripped = line.strip()
            if "input_columns:" in stripped:
                in_input_columns = True
                continue
            if in_input_columns:
                if stripped.startswith("-"):
                    needs_update = True
                    print(f"Found list format at line {line_num}: {stripped}")
                    break
                if stripped and not stripped.startswith("#"):
                    if ":" in stripped:
                        break
                if not stripped:
                    in_input_columns = False

        if not needs_update:
            print(f"Skipping {file_path} - already in new format")
            return

        # only if we need to update certain files, we can then convert.
        new_content = []
        in_input_columns = False

        for line in yaml_content:
            if line.strip().startswith("#"):
                new_content.append(line)
                continue

            if "input_columns:" in line:
                in_input_columns = True
                new_content.append("input_columns:\n")
                for column_name, column_type in schema:
                    new_content.append(f"  {column_name}: {column_type}\n")
            elif not line.strip().startswith("-") and in_input_columns:
                in_input_columns = False
                new_content.append(line)
            elif not in_input_columns:
                new_content.append(line)

        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(new_content)

    def process_task(self, task: UpdateTask, is_test_update: bool = False) -> None:
        """Process a single update task."""
        print(f"Processing file: {task.file_path}")
        if is_test_update:
            with open(task.file_path, "r", encoding="utf-8") as f:
                yaml_content = yaml.safe_load(f)

            input_columns = yaml_content.get("input_columns", {})
            schema = [(name, "STRING") for name in input_columns]
        else:
            # For non-test files, fetch the schema from BigQuery
            if not isinstance(task.state_code, StateCode):
                raise ValueError(
                    "State code must be a real state for non-test updates."
                )

            if task.state_code is None:
                raise ValueError("State code must be provided for non-test updates.")

            table_address = self.get_table_address(
                state_code=task.state_code, ingest_view_name=task.ingest_view_name
            )
            schema = self.get_bigquery_schema(table_address)
            if not schema:
                raise ValueError(
                    f"Found no columns in the schema for ingest view table"
                    f"{table_address.to_str()}"
                )

        self.update_yaml_file(task.file_path, schema)


class PathType(Enum):
    PRODUCTION = "production"
    TEST = "test"
    FAKE = "fake"


class PathStrategy(ABC):
    """Abstract base class for different path strategies"""

    @abstractmethod
    def get_base_path(self) -> str:
        pass

    @abstractmethod
    def collect_tasks(self) -> List[UpdateTask]:
        pass


class ProductionPathStrategy(PathStrategy):
    def get_base_path(self) -> str:
        return "recidiviz/ingest/direct/regions"

    def collect_tasks(self) -> List[UpdateTask]:
        tasks = []
        states = get_direct_ingest_states_existing_in_env()
        for state_code in states:
            region = direct_ingest_regions.get_direct_ingest_region(
                region_code=state_code.value.lower()
            )
            collector = IngestViewManifestCollector(
                region=region,
                delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
            )
            for (
                ingest_view_name,
                file_path,
            ) in collector.ingest_view_to_manifest_path.items():
                tasks.append(UpdateTask(state_code, ingest_view_name, file_path))
            print(
                f"Found {len(collector.ingest_view_to_manifest_path)} mapping files for {state_code}"
            )
        return tasks


class TestPathStrategy(PathStrategy):
    def get_base_path(self) -> str:
        return "recidiviz/tests/ingest/direct/ingest_mappings/fixtures/ingest_view_file_parser/manifests"

    def collect_tasks(self) -> List[UpdateTask]:
        tasks = []
        for file_path in glob.glob(os.path.join(self.get_base_path(), "*.yaml")):
            ingest_view_name = os.path.splitext(os.path.basename(file_path))[0]
            tasks.append(UpdateTask(None, ingest_view_name, file_path))
        return tasks


class FakePathStrategy(PathStrategy):
    def get_base_path(self) -> str:
        return "recidiviz/tests/ingest/direct/fake_regions"

    def collect_tasks(self) -> List[UpdateTask]:
        tasks = []
        for fake_region_dir in glob.glob(os.path.join(self.get_base_path(), "us_*")):
            state_code = os.path.basename(fake_region_dir)[3:5].upper()
            mappings_dir = os.path.join(fake_region_dir, "ingest_mappings")
            if not os.path.exists(mappings_dir):
                continue
            for file_path in glob.glob(os.path.join(mappings_dir, "*.yaml")):
                ingest_view_name = os.path.splitext(os.path.basename(file_path))[
                    0
                ].split("_")[-1]
                tasks.append(UpdateTask(state_code, ingest_view_name, file_path))
        return tasks


class ParallelSchemaUpdater:
    """A class that handles parallel updating of ingest mapping YAML files with schema information.

    This class coordinates the parallel processing of YAML mapping files to update their schema format.
    It can handle three types of updates:

    1. Production state files - Updates with actual BigQuery schema types
    2. Test fixture files - Assigns placeholder STRING type to all columns
    3. Fake region files - Assigns placeholder STRING type to all columns

    The updater uses multiprocessing to parallelize the work across multiple processes for improved performance.

    Attributes:
        path_strategy: The strategy object that determines which files to process based on path type
        expected_num_ingest_views: Expected number of mapping files to validate completeness
        num_processes: Number of parallel processes to use for updates

    Args:
        path_type: Enum indicating the type of paths to process (PRODUCTION/TEST/FAKE)
        num_processes: Number of parallel processes to use (default: 6)
        expected_num_ingest_views: Expected number of mapping files to process
    """

    def __init__(
        self,
        path_type: PathType,
        num_processes: int = 6,
        expected_num_ingest_views: int = 218,
    ) -> None:
        self.path_strategy = self._get_path_strategy(path_type)
        self.expected_num_ingest_views = expected_num_ingest_views
        self.num_processes = num_processes

    @staticmethod
    def _get_path_strategy(path_type: PathType) -> PathStrategy:
        strategies = {
            PathType.PRODUCTION: ProductionPathStrategy(),
            PathType.TEST: TestPathStrategy(),
            PathType.FAKE: FakePathStrategy(),
        }
        return strategies[path_type]

    def run_update(self) -> None:
        """Main function to run the parallel schema update process."""
        tasks = self.get_all_mapping_files()

        assert len(tasks) == self.expected_num_ingest_views, (
            f"Got {len(tasks)} tasks but expected {self.expected_num_ingest_views} ingest views. "
            f"This might indicate missing or extra mapping files."
        )

        # Create a pool of worker processes and map tasks to it
        # ands set up a path strategy if tests/fake are inferred by the user
        # given arguments.

        worker = SchemaUpdaterWorker()
        is_test_update = isinstance(
            self.path_strategy, (TestPathStrategy, FakePathStrategy)
        )

        with mp.Pool(processes=self.num_processes) as pool:
            pool.starmap(
                worker.process_task, [(task, is_test_update) for task in tasks]
            )

        print(f"Successfully processed {len(tasks)} files")

    def get_all_mapping_files(self) -> List[UpdateTask]:
        return self.path_strategy.collect_tasks()


def parse_args() -> Any:
    parser = argparse.ArgumentParser(
        description="Updates ingest mapping YAML files to include column types from BigQuery schema."
    )
    parser.add_argument(
        "--path-type",
        type=PathType,
        choices=list(PathType),
        required=True,
        help="Type of paths to process (production/test/fake)",
    )

    parser.add_argument(
        "--num-processes",
        type=int,
        default=6,
        help="The number of parallel processes to use.",
    )

    parser.add_argument(
        "--expected-yaml-files",
        type=int,
        required=True,
        help="The expected number of YAML files to process.",
    )

    return parser.parse_args()


def main() -> None:
    """Entry point for the parallel schema update script."""
    print("Starting parallel schema update process...")

    args = parse_args()
    updater = ParallelSchemaUpdater(
        path_type=args.path_type,
        num_processes=args.num_processes,
        expected_num_ingest_views=args.expected_yaml_files,
    )
    updater.run_update()
    print("Schema update process completed!")


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        main()
