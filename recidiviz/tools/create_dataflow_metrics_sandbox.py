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
"""Creates a new dataflow_metrics dataset prefixed with the provided sandbox_dataset_prefix, where the schema of the
metric tables will match the attributes on the metric classes locally. This script is used to create a test output
location when testing Dataflow calculation changes. The sandbox_dataset_prefix provided should be your github username
or some personal unique identifier so it's easy for others to tell who created the dataset.

Run locally with the following command:

    python -m recidiviz.tools.create_dataflow_metrics_sandbox \
        --project_id [PROJECT_ID] \
        --sandbox_dataset_prefix [SANDBOX_DATASET_PREFIX]
"""
import argparse
import logging
import sys
from typing import Tuple, List

from recidiviz.big_query.big_query_client import BigQueryClientImpl

from recidiviz.calculator.dataflow_metric_table_manager import update_dataflow_metric_tables_schemas
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

# When creating temporary datasets with prefixed names, set the default table expiration to 72 hours
TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS = 72 * 60 * 60 * 1000


def create_dataflow_metrics_sandbox(sandbox_dataset_prefix: str) -> None:
    sandbox_dataset_id = sandbox_dataset_prefix + '_' + dataset_config.DATAFLOW_METRICS_DATASET

    bq_client = BigQueryClientImpl()
    sandbox_dataset_ref = bq_client.dataset_ref_for_id(sandbox_dataset_id)

    if bq_client.dataset_exists(sandbox_dataset_ref):
        raise ValueError(f"{sandbox_dataset_id} already exists in project {bq_client.project_id}. Cannot create a "
                         f"Dataflow sandbox in an existing dataset.")

    logging.info("Creating dataflow metrics sandbox in dataset %s. Tables will expire after 72 hours.",
                 sandbox_dataset_ref)
    bq_client.create_dataset_if_necessary(sandbox_dataset_ref, TEMP_DATAFLOW_DATASET_DEFAULT_TABLE_EXPIRATION_MS)

    update_dataflow_metric_tables_schemas(dataflow_metrics_dataset_id=sandbox_dataset_id)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to call the desired function."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
                        required=True)
    parser.add_argument('--sandbox_dataset_prefix',
                        dest='sandbox_dataset_prefix',
                        type=str,
                        required=True,
                        help="A prefix to append to the dataflow_metrics dataset where the sandbox metrics tables will"
                             " be loaded. Should be your github username or some personal unique identifier so it's"
                             " easy for others to tell who created the dataset.")

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        create_dataflow_metrics_sandbox(known_args.sandbox_dataset_prefix)
