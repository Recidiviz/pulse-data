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
"""Creates new dataflow pipeline datasets (normalized state sandboxes, dataflow metrics,
supplemental datasets) prefixed with the provided sandbox_dataset_prefix, where the schemas
of the output tables will match the attributes on the classes locally. This script is used
to create a test output location when testing Dataflow calculation changes. The
sandbox_dataset_prefix provided should be your github username or some personal unique
identifier so it's easy for others to tell who created the dataset.

Run locally with the following command:

python -m recidiviz.tools.calculator.create_or_update_dataflow_sandbox \
        --project_id [PROJECT_ID] \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
        [--allow_overwrite] \
        --datasets_to_create metrics normalization supplemental (must be last due to list)
"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.dataflow_output_table_manager import (
    get_metric_pipeline_enabled_states,
    get_state_specific_normalized_state_dataset_for_state,
    update_dataflow_metric_tables_schemas,
    update_normalized_state_schema,
    update_supplemental_dataset_schemas,
)
from recidiviz.calculator.pipeline.supplemental.dataset_config import (
    SUPPLEMENTAL_DATA_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# When creating temporary datasets with prefixed names, set the default table
# expiration to 72 hours
TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS = 72 * 60 * 60 * 1000

SANDBOX_TYPES = ["metrics", "normalization", "supplemental"]


def create_or_update_supplemental_datasets_sandbox(
    bq_client: BigQueryClientImpl,
    sandbox_dataset_prefix: str,
    allow_overwrite: bool = False,
) -> None:
    """Creates or updates a supplemental dataset sandbox dataset, prefixing the dataset
    name with the given prefix."""
    sandbox_dataset_id = sandbox_dataset_prefix + "_" + SUPPLEMENTAL_DATA_DATASET

    sandbox_dataset_ref = bq_client.dataset_ref_for_id(sandbox_dataset_id)

    if bq_client.dataset_exists(sandbox_dataset_ref) and not allow_overwrite:
        logging.error(
            "Dataset %s already exists in project %s. To overwrite, set --allow_overwrite.",
            sandbox_dataset_id,
            bq_client.project_id,
        )
        sys.exit(1)

    logging.info(
        "Creating dataflow metrics sandbox in dataset %s. Tables will expire after 72 hours.",
        sandbox_dataset_ref,
    )
    bq_client.create_dataset_if_necessary(
        sandbox_dataset_ref, TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS
    )

    update_supplemental_dataset_schemas(
        supplemental_metrics_dataset_id=sandbox_dataset_id
    )


def create_or_update_dataflow_metrics_sandbox(
    bq_client: BigQueryClientImpl,
    sandbox_dataset_prefix: str,
    allow_overwrite: bool = False,
) -> None:
    """Creates or updates a Dataflow metrics sandbox dataset, prefixing the dataset name with the given prefix."""
    sandbox_dataset_id = sandbox_dataset_prefix + "_" + DATAFLOW_METRICS_DATASET

    sandbox_dataset_ref = bq_client.dataset_ref_for_id(sandbox_dataset_id)

    if bq_client.dataset_exists(sandbox_dataset_ref) and not allow_overwrite:
        logging.error(
            "Dataset %s already exists in project %s. To overwrite, set --allow_overwrite.",
            sandbox_dataset_id,
            bq_client.project_id,
        )
        sys.exit(1)

    logging.info(
        "Creating dataflow metrics sandbox in dataset %s. Tables will expire after 72 hours.",
        sandbox_dataset_ref,
    )
    bq_client.create_dataset_if_necessary(
        sandbox_dataset_ref, TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS
    )

    update_dataflow_metric_tables_schemas(
        dataflow_metrics_dataset_id=sandbox_dataset_id
    )


def create_or_update_normalized_state_sandbox(
    bq_client: BigQueryClientImpl,
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    allow_overwrite: bool = False,
) -> None:
    """Creates or updates a sandbox normalized_state datasets, prefixing the datasets
    name with the given prefix. Creates one dataset per state_code that has
    calculation pipelines regularly scheduled."""

    # First create the sandbox dataset with the default table expiration
    sandbox_dataset_id = get_state_specific_normalized_state_dataset_for_state(
        state_code=state_code, normalized_dataset_prefix=sandbox_dataset_prefix
    )

    sandbox_dataset_ref = bq_client.dataset_ref_for_id(sandbox_dataset_id)
    if bq_client.dataset_exists(sandbox_dataset_ref) and not allow_overwrite:
        logging.error(
            "Dataset %s already exists in project %s. To overwrite, "
            "set --allow_overwrite.",
            sandbox_dataset_id,
            bq_client.project_id,
        )
        sys.exit(1)

    bq_client.create_dataset_if_necessary(
        sandbox_dataset_ref, TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS
    )

    logging.info(
        "Created normalized state sandbox datasets with prefix %s. Tables will expire "
        "after 72 hours.",
        sandbox_dataset_prefix,
    )
    update_normalized_state_schema(
        state_code=state_code, normalized_dataset_prefix=sandbox_dataset_prefix
    )


def create_or_update_dataflow_sandbox(
    sandbox_dataset_prefix: str,
    datasets_to_create: List[str],
    allow_overwrite: bool = False,
) -> None:
    """Creates or updates a sandbox for all the pipeline types, prefixing the dataset
    name with the given prefix. Creates one dataset per state_code that has
    calculation pipelines regularly scheduled."""
    bq_client = BigQueryClientImpl()

    # First create the sandbox normalized datasets
    if "normalization" in datasets_to_create:
        for state_code in get_metric_pipeline_enabled_states():
            create_or_update_normalized_state_sandbox(
                bq_client,
                StateCode(state_code),
                sandbox_dataset_prefix,
                allow_overwrite,
            )

    # Then create the sandbox metric datasets
    if "metrics" in datasets_to_create:
        create_or_update_dataflow_metrics_sandbox(
            bq_client, sandbox_dataset_prefix, allow_overwrite
        )

    # Then create the supplemental datasets
    if "supplemental" in datasets_to_create:
        create_or_update_supplemental_datasets_sandbox(
            bq_client, sandbox_dataset_prefix, allow_overwrite
        )


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
    parser.add_argument(
        "--sandbox_dataset_prefix",
        dest="sandbox_dataset_prefix",
        type=str,
        required=True,
        help="A prefix to append to the normalized_state datasets. Should be your "
        "github username or some personal unique identifier so it's easy for "
        "others to tell who created the dataset.",
    )
    parser.add_argument(
        "--allow_overwrite",
        dest="allow_overwrite",
        action="store_true",
        default=False,
        help="Allow existing datasets to be overwritten.",
    )
    parser.add_argument(
        "--datasets_to_create",
        dest="datasets_to_create",
        type=str,
        nargs="+",
        choices=SANDBOX_TYPES,
        help="A list of the types of sandboxes to create",
        default=SANDBOX_TYPES,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        create_or_update_dataflow_sandbox(
            known_args.sandbox_dataset_prefix,
            known_args.datasets_to_create,
            known_args.allow_overwrite,
        )
