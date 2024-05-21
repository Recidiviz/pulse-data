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
        [--state_code STATE_CODE] \
        [--ingest_instance INGEST_INSTANCE] \
        [--allow_overwrite] \
        --datasets_to_create metrics normalization supplemental ingest (must be last due to list)
"""
import argparse
import logging
import sys
from typing import List, Optional

from recidiviz.airflow.dags.utils.dag_orchestration_utils import (
    get_ingest_pipeline_enabled_state_and_instance_pairs,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataset,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    update_bq_dataset_to_match_sqlalchemy_schema,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.pipelines.dataflow_orchestration_utils import (
    get_normalization_pipeline_enabled_states,
)
from recidiviz.pipelines.dataflow_output_table_manager import (
    update_dataflow_metric_tables_schemas,
    update_normalized_table_schemas_in_dataset,
    update_state_specific_ingest_view_result_schema,
    update_supplemental_dataset_schemas,
)
from recidiviz.pipelines.ingest.dataset_config import state_dataset_for_state_code
from recidiviz.pipelines.normalization.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# When creating temporary datasets with prefixed names, set the default table
# expiration to 72 hours
TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS = 72 * 60 * 60 * 1000

SANDBOX_TYPES = ["metrics", "normalization", "supplemental", "ingest"]


def create_or_update_ingest_output_sandbox(
    bq_client: BigQueryClientImpl,
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    sandbox_dataset_prefix: str,
    allow_overwrite: bool = False,
) -> None:
    """Creates or updates a sandbox state and ingest view results datasets, prefixing the
    datasets name with the given prefix. Creates one dataset per state_code and ingest instance
    that has calculation pipelines regularly scheduled."""

    sandbox_ingest_view_results_id = ingest_view_materialization_results_dataset(
        state_code, ingest_instance, sandbox_dataset_prefix
    )
    sandbox_ingest_view_results_ref = bq_client.dataset_ref_for_id(
        sandbox_ingest_view_results_id
    )
    if (
        bq_client.dataset_exists(sandbox_ingest_view_results_ref)
        and not allow_overwrite
    ):
        logging.error(
            "Dataset %s already exists in project %s. To overwrite, "
            "set --allow_overwrite.",
            sandbox_ingest_view_results_id,
            bq_client.project_id,
        )
        sys.exit(1)
    logging.info(
        "Creating state ingest view result sandbox datasets with prefix %s. Tables will expire "
        "after 72 hours.",
        sandbox_dataset_prefix,
    )
    update_state_specific_ingest_view_result_schema(
        sandbox_ingest_view_results_id,
        state_code,
        ingest_instance,
        default_table_expiration_ms=TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )

    sandbox_state_id = state_dataset_for_state_code(
        state_code, ingest_instance, sandbox_dataset_prefix
    )
    sandbox_state_ref = bq_client.dataset_ref_for_id(sandbox_state_id)
    if bq_client.dataset_exists(sandbox_state_ref) and not allow_overwrite:
        logging.error(
            "Dataset %s already exists in project %s. To overwrite, "
            "set --allow_overwrite.",
            sandbox_state_id,
            bq_client.project_id,
        )
        sys.exit(1)
    logging.info(
        "Creating state sandbox datasets with prefix %s. Tables will expire "
        "after 72 hours.",
        sandbox_dataset_prefix,
    )
    update_bq_dataset_to_match_sqlalchemy_schema(
        schema_type=SchemaType.STATE,
        dataset_id=sandbox_state_id,
        default_table_expiration_ms=TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )


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
        "Creating supplemental dataset sandbox in dataset %s. Tables will expire after 72 hours.",
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
    sandbox_dataset_id = f"{sandbox_dataset_prefix}_{normalized_state_dataset_for_state_code(state_code)}"

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
        "Creating normalized state sandbox datasets with prefix %s. Tables will expire "
        "after 72 hours.",
        sandbox_dataset_prefix,
    )

    update_normalized_table_schemas_in_dataset(
        normalized_state_dataset_id=sandbox_dataset_id,
        default_table_expiration_ms=TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )


# TODO(#23142): Merge underlying dataset creation logic with functions in update_big_query_table_schemas.py
def _create_or_update_dataflow_sandbox(
    sandbox_dataset_prefix: str,
    datasets_to_create: List[str],
    allow_overwrite: bool,
    state_code_filter: Optional[StateCode],
    ingest_instance_filter: Optional[DirectIngestInstance],
) -> None:
    """Creates or updates a sandbox for all the pipeline types, prefixing the dataset
    name with the given prefix. Creates one dataset per state_code that has
    calculation pipelines regularly scheduled."""
    bq_client = BigQueryClientImpl()

    # Create the ingest view results dataset and state datasets:
    if "ingest" in datasets_to_create:
        state_instance_pairs = {
            (state_code, instance)
            for state_code, instance in get_ingest_pipeline_enabled_state_and_instance_pairs()
            if (not state_code_filter or state_code == state_code_filter)
            and (not ingest_instance_filter or instance == ingest_instance_filter)
        }

        for state_code, ingest_instance in state_instance_pairs:
            create_or_update_ingest_output_sandbox(
                bq_client,
                state_code,
                ingest_instance,
                sandbox_dataset_prefix,
                allow_overwrite,
            )

    # Create the sandbox normalized datasets
    if "normalization" in datasets_to_create:
        state_codes = (
            {state_code_filter}
            if state_code_filter
            else get_normalization_pipeline_enabled_states()
        )

        for state_code in state_codes:
            create_or_update_normalized_state_sandbox(
                bq_client,
                state_code,
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


def parse_arguments() -> argparse.Namespace:
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
        "--state_code",
        type=StateCode,
        help="If set, sandbox datasets will only be created for this state. Relevant "
        "when creating the normalization dataset.",
    )
    parser.add_argument(
        "--ingest_instance",
        type=DirectIngestInstance,
        help="If set, sandbox datasets"
        "will only be created for this ingest instance. Relevant when creating the ingest datasets.",
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

    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments()

    with local_project_id_override(args.project_id):
        _create_or_update_dataflow_sandbox(
            args.sandbox_dataset_prefix,
            args.datasets_to_create,
            args.allow_overwrite,
            args.state_code,
            args.ingest_instance,
        )
