# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Helper functions to create and update BigQuery Views."""

import concurrent
import logging

from google.cloud import bigquery, exceptions

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient

_BQ_LOAD_WAIT_TIMEOUT_SECONDS = 300


def wait_for_table_load(
    big_query_client: BigQueryClient, load_job: bigquery.job.LoadJob
) -> bool:
    """Wait for a table LoadJob to finish, and log its status.

    Args:
        big_query_client: A BigQueryClient for querying the result table
        load_job: BigQuery LoadJob whose result to wait for.
    Returns:
        True if no errors were raised, else False.
    """
    try:
        # Wait for table load job to complete.
        load_job.result(timeout=_BQ_LOAD_WAIT_TIMEOUT_SECONDS)

        destination_table_address = ProjectSpecificBigQueryAddress.from_table_reference(
            load_job.destination
        )

        logging.info(
            "Load job %s for table %s completed successfully.",
            load_job.job_id,
            destination_table_address.to_str(),
        )

        destination_table = big_query_client.get_table(
            destination_table_address.to_project_agnostic_address()
        )
        logging.info(
            "Loaded %d rows in table %s.%s.%s",
            destination_table.num_rows,
            load_job.destination.project,
            load_job.destination.dataset_id,
            load_job.destination.table_id,
        )
        return True
    except (
        exceptions.NotFound,
        exceptions.BadRequest,
        concurrent.futures.TimeoutError,
    ):  # type: ignore
        logging.exception(
            "Failed to load table %s.%s.%s",
            load_job.destination.project,
            load_job.destination.dataset_id,
            load_job.destination.table_id,
        )
        return False
