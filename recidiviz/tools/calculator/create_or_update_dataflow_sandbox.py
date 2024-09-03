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
        [--allow_overwrite] \
        --datasets_to_create metrics normalization supplemental ingest (must be last due to list)
"""
import argparse
import logging
import os
from typing import List

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.pipelines.pipeline_names import (
    INGEST_PIPELINE_NAME,
    METRICS_PIPELINE_NAME,
    NORMALIZATION_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_view_source_table_configs,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollection,
    StateSpecificSourceTableLabel,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# When creating temporary datasets with prefixed names, set the default table
# expiration to 72 hours
TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS = 72 * 60 * 60 * 1000

SANDBOX_TYPES = [
    METRICS_PIPELINE_NAME,
    # TODO(#31741): Remove this and update docstring once combined ingest and
    #  normalization pipelines are launched in all states.
    NORMALIZATION_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
    INGEST_PIPELINE_NAME,
]


def create_or_update_source_table_collections(
    source_table_collections: list[SourceTableCollection], allow_overwrite: bool = False
) -> None:
    bq_client = BigQueryClientImpl()
    for source_table_collection in source_table_collections:
        sandbox_dataset_id = source_table_collection.dataset_id

        if bq_client.dataset_exists(sandbox_dataset_id) and not allow_overwrite:
            raise ValueError(
                f"Dataset {sandbox_dataset_id} already exists in project {bq_client.project_id}. To overwrite, set --allow_overwrite.",
            )

    if not all(
        source_table_collection.is_sandbox_collection
        for source_table_collection in source_table_collections
    ):
        raise ValueError(
            "Cannot create sandboxed datasets for non-sandboxed collections"
        )

    update_manager = SourceTableUpdateManager()
    update_manager.update_async(
        source_table_collections=source_table_collections,
        log_file=os.path.join(
            os.path.dirname(__file__),
            "logs/create_or_update_sandbox.log",
        ),
        log_output=False,
    )


def create_or_update_dataflow_sandbox(
    sandbox_dataset_prefix: str,
    pipelines: List[str],
    allow_overwrite: bool,
    state_code_filter: StateCode | None = None,
) -> None:
    """Creates or updates a sandbox for all the pipelines specified, prefixing the
    dataset name with the given prefix. Creates one dataset per state_code that has
    calculation pipelines regularly scheduled."""
    dataflow_source_tables = SourceTableRepository(
        source_table_collections=get_dataflow_output_source_table_collections()
    )
    collections_to_create: list[SourceTableCollection] = []

    state_codes = [
        state_code
        for state_code in get_direct_ingest_states_existing_in_env()
        if (not state_code_filter or state_code == state_code_filter)
    ]

    for pipeline in pipelines:
        pipeline_collections = dataflow_source_tables.get_collections(
            labels=[DataflowPipelineSourceTableLabel(pipeline_name=pipeline)]
        )

        if pipeline == INGEST_PIPELINE_NAME:
            # TODO(#30495): These will not need to be added separately once ingest views
            #  define their schemas in the YAML mappings definitions and we can collect
            #  these ingest view tables with all the other source tables.
            collections_to_create.extend(
                build_ingest_view_source_table_configs(
                    bq_client=BigQueryClientImpl(),
                    state_codes=state_codes,
                )
            )

        # Filter down to relevant collections based on filters
        if pipeline in (
            INGEST_PIPELINE_NAME,
            # TODO(#31741): Remove this and update docstring once combined ingest and
            #  normalization pipelines are launched in all states.
            NORMALIZATION_PIPELINE_NAME,
        ):
            pipeline_collections = [
                c
                for c in pipeline_collections
                if c.has_any_label(
                    [
                        StateSpecificSourceTableLabel(state_code=state_code)
                        for state_code in state_codes
                    ]
                )
            ]

        collections_to_create.extend(pipeline_collections)

    create_or_update_source_table_collections(
        source_table_collections=[
            collection.as_sandbox_collection(
                sandbox_dataset_prefix=sandbox_dataset_prefix
            )
            for collection in collections_to_create
        ],
        allow_overwrite=allow_overwrite,
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
        create_or_update_dataflow_sandbox(
            args.sandbox_dataset_prefix,
            args.datasets_to_create,
            args.allow_overwrite,
            args.state_code,
        )
