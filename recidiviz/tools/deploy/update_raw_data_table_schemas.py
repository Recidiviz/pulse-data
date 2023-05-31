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
from typing import Any, Dict, List, Tuple

from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClient,
    BigQueryClientImpl,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import raw_data_table_schema_utils
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_new_raw_data_dataset,
    raw_data_pruning_raw_data_diff_results_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.deploy.logging import get_deploy_logs_dir, redirect_logging_to_file
from recidiviz.tools.utils.script_helpers import (
    interactive_loop_until_tasks_succeed,
    interactive_prompt_retry_on_exception,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results
from recidiviz.utils.metadata import local_project_id_override

TEN_MINUTES = 60 * 10

ONE_DAY_MS = 24 * 60 * 60 * 1000


def create_states_raw_data_datasets_if_necessary(
    state_codes: List,
    bq_client: BigQueryClient,
) -> None:
    """
    Create primary and secondary raw datasets for each state
    in the environment, if it does not already exist
    """
    logging.info("Creating raw data datasets (if necessary)...")
    for state_code in state_codes:
        for instance in DirectIngestInstance:
            _create_raw_data_datasets(state_code, instance, bq_client)


def _create_raw_data_datasets(
    state_code: StateCode, instance: DirectIngestInstance, bq_client: BigQueryClient
) -> None:
    """Create raw data datasets for a given state code and instance."""
    dataset_ids = [
        raw_tables_dataset_for_region(state_code=state_code, instance=instance),
        # For a given state and instance, create the raw datasets used for housing temporary tables related to
        # raw data pruning. The tables within the dataset will be temporarily added and deleted in the process of
        # raw data pruning, but the datasets themselves won't.
        raw_data_pruning_new_raw_data_dataset(state_code, instance),
        raw_data_pruning_raw_data_diff_results_dataset(state_code, instance),
    ]
    for dataset_id in dataset_ids:
        dataset_ref = bq_client.dataset_ref_for_id(dataset_id)
        # pylint: disable=cell-var-from-loop
        # TODO(PyCQA/pylint#5263): This is a bug in pylint, we can remove once the bug is fixed
        interactive_prompt_retry_on_exception(
            fn=lambda: bq_client.create_dataset_if_necessary(
                dataset_ref,
                # Set default table expiration time for datasets used
                # for raw data pruning
                ONE_DAY_MS if dataset_id.startswith("pruning") else None,
            ),
            input_text=(
                f"The attempt to create {dataset_id} failed. " f"Should we try again?"
            ),
            accepted_response_override="yes",
            exit_on_cancel=False,
        )


def update_raw_data_table_schemas(
    file_kwargs: List[Dict[str, Any]],
    log_path: str,
) -> None:
    """
    Update schemas for raw datasets
    """
    logging.info("Writing logs to %s", log_path)

    def update_raw_tables_with_redirect(
        file_kwargs: List[Dict[str, Any]], log_path: str
    ) -> Tuple[
        List[Tuple[Any, Dict[str, Any]]], List[Tuple[Exception, Dict[str, Any]]]
    ]:
        with redirect_logging_to_file(log_path):
            return map_fn_with_progress_bar_results(
                fn=raw_data_table_schema_utils.update_raw_data_table_schema,
                kwargs_list=file_kwargs,
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=TEN_MINUTES,
                progress_bar_message="Updating raw table schemas...",
            )

    interactive_loop_until_tasks_succeed(
        tasks_fn=lambda tasks_kwargs: update_raw_tables_with_redirect(
            file_kwargs=tasks_kwargs, log_path=log_path
        ),
        tasks_kwargs=file_kwargs,
    )


def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    project_id = parser.parse_args().project_id
    with local_project_id_override(project_id):
        state_codes = get_direct_ingest_states_existing_in_env()
        bq_client = BigQueryClientImpl()
        logging.info("Getting raw file configs...")
        file_kwargs = [
            {
                "state_code": state_code,
                "raw_file_tag": raw_file_tag,
                "instance": instance,
                "big_query_client": bq_client,
            }
            for state_code in state_codes
            for instance in DirectIngestInstance
            for raw_file_tag in get_region_raw_file_config(
                state_code.value
            ).raw_file_configs
        ]
        create_states_raw_data_datasets_if_necessary(
            state_codes=state_codes, bq_client=bq_client
        )
        update_raw_data_table_schemas(
            file_kwargs=file_kwargs,
            log_path=os.path.join(
                get_deploy_logs_dir(), "update_raw_data_table_schemas.log"
            ),
        )


if __name__ == "__main__":
    main()
