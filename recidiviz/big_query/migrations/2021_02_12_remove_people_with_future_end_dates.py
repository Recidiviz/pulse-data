# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Deletes all rows where a person has an incarceration population or supervision population metric
for after today.

*** THIS SCRIPT SHOULD ONLY BE RUN BY THE ON-CALL ENGINEER AT THE TIME OF THE FIRST DEPLOY AFTER THIS LANDS ***

Run with the following command:
    python -m recidiviz.big_query.migrations.2021_02_12_remove_people_with_future_end_dates \
        --project_id [PROJECT_ID]
        --dry_run [DRY_RUN]

"""
import argparse
import logging
import sys
from typing import List, Tuple

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def delete_from_table(
    dry_run: bool, bq_client: BigQueryClient, table_id: str, filter_clause: str
) -> None:
    if dry_run:
        logging.info(
            "[DRY RUN] Would delete rows from [%s].[%s] %s",
            DATAFLOW_METRICS_DATASET,
            table_id,
            filter_clause,
        )
    else:
        # Delete these rows from the Dataflow metrics table
        delete_job = bq_client.delete_from_table_async(
            DATAFLOW_METRICS_DATASET, table_id, filter_clause=filter_clause
        )

        # Wait for the delete job to complete before moving on
        delete_job.result()


def main(dry_run: bool) -> None:
    """Deletes all rows where a person has an incarceration population or supervision population metric
    for after today."""
    bq_client = BigQueryClientImpl()

    incarceration_table = "incarceration_population_metrics"
    incarceration_filter = "WHERE date_of_stay > CURRENT_DATE('US/Eastern')"
    delete_from_table(dry_run, bq_client, incarceration_table, incarceration_filter)

    supervision_table = "supervision_population_metrics"
    supervision_filter = "WHERE date_of_supervision > CURRENT_DATE('US/Eastern')"
    delete_from_table(dry_run, bq_client, supervision_table, supervision_filter)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument("--dry_run", dest="dry_run", type=str_to_bool, required=True)

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    logging.warning(
        "This script should only be run by the on-call engineer doing a deploy."
        " Are you the on-call engineer running a deploy for the %s project? (Y/n)",
        known_args.project_id,
    )
    continue_running = input()
    if continue_running != "Y":
        sys.exit()

    with local_project_id_override(known_args.project_id):
        main(known_args.dry_run)
