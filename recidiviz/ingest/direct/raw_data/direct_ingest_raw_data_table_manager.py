# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Manages raw data table schema updates based on the YAML files defined in source code."""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def update_raw_data_tables_schemas_in_dataset(state_code: str) -> None:
    """Update the raw data tables for a given state by obtaining the raw file configs
    for a state and updating the schema based on the columns defined in the YAMLs. In
    addition, adds the necessary file_id and update_datetime columns to the schema."""
    bq_client = BigQueryClientImpl()

    raw_data_dataset_id = raw_tables_dataset_for_region(state_code)
    region_config = get_region_raw_file_config(state_code)
    raw_data_dataset_ref = bq_client.dataset_ref_for_id(raw_data_dataset_id)

    bq_client.create_dataset_if_necessary(raw_data_dataset_ref)

    for raw_file_tag, raw_data_config in region_config.raw_file_configs.items():
        columns = [column.name for column in raw_data_config.columns] + [
            FILE_ID_COL_NAME,
            UPDATE_DATETIME_COL_NAME,
        ]
        schema = DirectIngestRawFileImportManager.create_raw_table_schema_from_columns(
            columns
        )
        if bq_client.table_exists(raw_data_dataset_ref, raw_file_tag):
            bq_client.update_schema(
                raw_data_dataset_id, raw_file_tag, schema, allow_field_deletions=False
            )
        else:
            bq_client.create_table_with_schema(
                raw_data_dataset_id, raw_file_tag, schema
            )


def update_raw_data_tables_schemas() -> None:
    """Update the raw data tables for all states that have support for direct ingest."""
    for state_code in get_direct_ingest_states_existing_in_env():
        logging.info("Updating schemas for state %s", state_code.value)
        update_raw_data_tables_schemas_in_dataset(state_code.value)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to call the desired function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        update_raw_data_tables_schemas()
