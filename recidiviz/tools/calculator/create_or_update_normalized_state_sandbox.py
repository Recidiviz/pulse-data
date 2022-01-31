# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Creates a new us_xx_normalized_state dataset prefixed with the provided
sandbox_dataset_prefix. This script is used to create a test output location when
testing Dataflow entity normalization changes. The sandbox_dataset_prefix provided
should be your github username or some personal unique identifier so it's easy for
others to tell who created the datasets.

Run locally with the following command:

    python -m recidiviz.tools.calculator.create_or_update_normalized_state_sandbox \
        --project_id [PROJECT_ID] \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX] \
        --state_code \
        [--allow_overwrite]
"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.dataflow_output_table_manager import (
    get_state_specific_normalized_state_dataset_for_state,
    update_normalized_state_schema,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# When creating temporary datasets with prefixed names, set the default table
# expiration to 72 hours
TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS = 72 * 60 * 60 * 1000


def create_or_update_normalized_state_sandbox(
    state_code: StateCode, sandbox_dataset_prefix: str, allow_overwrite: bool = False
) -> None:
    """Creates or updates a sandbox normalized_state datasets, prefixing the datasets
    name with the given prefix. Creates one dataset per state_code that has
    calculation pipelines regularly scheduled."""
    bq_client = BigQueryClientImpl()

    # First create the sandbox dataset with the default table expiration
    sandbox_dataset_id = get_state_specific_normalized_state_dataset_for_state(
        state_code=state_code, normalized_dataset_prefix=sandbox_dataset_prefix
    )

    sandbox_dataset_ref = bq_client.dataset_ref_for_id(sandbox_dataset_id)
    if bq_client.dataset_exists(sandbox_dataset_ref) and not allow_overwrite:
        if __name__ == "__main__":
            logging.error(
                "Dataset %s already exists in project %s. To overwrite, "
                "set --allow_overwrite.",
                sandbox_dataset_id,
                bq_client.project_id,
            )
            sys.exit(1)
        else:
            raise ValueError(
                f"{sandbox_dataset_id} already exists in project "
                f"{bq_client.project_id}. Cannot create a normalized state sandbox "
                f"in an existing dataset."
            )

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
        "--state_code",
        dest="state_code",
        type=str,
        required=True,
        help="The state for which to create a sandbox normalized dataset.",
    )
    parser.add_argument(
        "--allow_overwrite",
        dest="allow_overwrite",
        action="store_true",
        default=False,
        help="Allow existing datasets to be overwritten.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    state = StateCode(known_args.state_code)

    print(state)

    with local_project_id_override(known_args.project_id):
        create_or_update_normalized_state_sandbox(
            state, known_args.sandbox_dataset_prefix, known_args.allow_overwrite
        )
