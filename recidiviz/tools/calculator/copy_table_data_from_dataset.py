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
"""
Copies data from a source table to a destination table for all tables within a dataset,
where it will overwrite the destination table completely. This expects the tables
to already exist in the destination dataset and will raise an error if they do not.
This script can be useful in testing the impact of schema changes on the table data
(e.g. performance impact of clustering changes).

Run locally with the following command:

    python -m recidiviz.tools.calculator.copy_table_data_from_dataset \
        --project_id [PROJECT_ID] \
        --source_dataset_id [SOURCE_DATASET_ID] \
        --destination_dataset_id [DESTINATION_DATASET_ID] \
"""

import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def copy_table_data_from_dataset(
    source_dataset_id: str, destination_dataset_id: str
) -> None:
    # Construct a BigQuery client object.
    bq_client = BigQueryClientImpl()
    jobs: List = []
    tables_in_source_table = bq_client.list_tables(source_dataset_id)

    for table_id in tables_in_source_table:
        # Set source_table_id to the ID of the original table.
        source_table_id = table_id.table_id

        # Set destination_table_id to the ID of the destination table.
        destination_table_id = table_id.table_id

        job = bq_client.insert_into_table_from_table_async(
            source_dataset_id,
            source_table_id,
            destination_dataset_id,
            destination_table_id,
        )
        jobs.append(job)

    for job in jobs:
        job.result()  # Wait for the job to complete.

    logging.info(
        "Data for all tables in [%s] copied to tables in [%s].",
        source_dataset_id,
        destination_dataset_id,
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
        "--source_dataset_id",
        dest="source_dataset_id",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--destination_dataset_id",
        dest="destination_dataset_id",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        copy_table_data_from_dataset(
            known_args.source_dataset_id, known_args.destination_dataset_id
        )
