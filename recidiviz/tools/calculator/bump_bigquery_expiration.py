# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tool to extend the expiration of tables in a sandbox to some number of days from now.

Note: This does not change the default expiration for the dataset, so any newly created
tables or views will still get an expiration based on the dataset default (e.g. 1 day).

Example:

python -m recidiviz.tools.calculator.bump_bigquery_expiration \
    --project-id recidiviz-staging \
    --dataset-prefix zbrenda \
    --days 3

"""
import argparse
import datetime
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.tools.utils.bigquery_helpers import run_operation_for_tables
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--dataset-prefix",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--days",
        type=int,
        required=True,
        help="Number of days from now that the tables should expire.",
    )

    return parser


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        expiration = datetime.datetime.now() + datetime.timedelta(days=args.days)
        run_operation_for_tables(
            client=BigQueryClientImpl(),
            prompt=f"Extend the expiration to {expiration.isoformat()}",
            operation=lambda client, dataset_id, table_id: client.set_table_expiration(
                dataset_id, table_id, expiration
            ),
            dataset_prefix=args.dataset_prefix,
        )
