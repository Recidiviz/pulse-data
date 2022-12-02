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
import os
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.ingest.direct import raw_data_table_schema_utils
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.deploy.logging import get_deploy_logs_dir
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.future_executor import map_fn_with_progress_bar
from recidiviz.utils.metadata import local_project_id_override


def update_raw_data_table_schemas() -> None:
    """Update the raw data tables for all states that have support for direct ingest."""
    state_codes = get_direct_ingest_states_existing_in_env()

    logging.info("Getting raw file configs...")
    file_kwargs = [
        {
            "state_code": state_code,
            "raw_file_tag": raw_file_tag,
            "instance": instance,
        }
        for state_code in state_codes
        for instance in DirectIngestInstance
        for raw_file_tag in get_region_raw_file_config(
            state_code.value
        ).raw_file_configs
    ]

    logging.info("Creating raw data datasets (if necessary)...")
    bq_client = BigQueryClientImpl()
    for state_code in state_codes:
        for instance in DirectIngestInstance:
            raw_data_dataset_id = raw_tables_dataset_for_region(
                state_code=state_code, instance=instance
            )
            raw_data_dataset_ref = bq_client.dataset_ref_for_id(raw_data_dataset_id)
            bq_client.create_dataset_if_necessary(raw_data_dataset_ref)

    log_path = os.path.join(get_deploy_logs_dir(), "update_raw_data_table_schemas.log")
    logging.info("Writing logs to %s", log_path)

    map_fn_with_progress_bar(
        fn=raw_data_table_schema_utils.update_raw_data_table_schema,
        kwargs_list=file_kwargs,
        progress_bar_message="Updating raw table schemas...",
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
        timeout_sec=(10 * 60),  # 10 minutes
        logging_redirect_filename=log_path,
    )

    logging.info("Update complete.")


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
        update_raw_data_table_schemas()
