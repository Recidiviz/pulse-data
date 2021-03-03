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
Script for cleaning up BigQuery temp tables that were abandoned by Dataflow and are
at least one day old.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.delete_temp_bq_tables --project-id recidiviz-staging --dry-run
"""
import argparse
from datetime import datetime, timedelta
import logging
import pytz

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delete temporary BigQuery datasets.")
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project against which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        help="If set, this indicates which datasets will be deleted without deleting them.",
        action="store_true",
    )
    return parser


def main(dry_run: bool) -> None:
    client = BigQueryClientImpl()
    datasets = list(client.list_datasets())
    candidate_deletable_datasets = [
        d for d in datasets if d.dataset_id.startswith("temp_dataset_")
    ]

    cutoff_date = (datetime.now() - timedelta(days=1)).replace(tzinfo=pytz.UTC)
    for candidate in candidate_deletable_datasets:
        dataset = client.get_dataset(candidate.dataset_id)
        if dataset.modified is not None and dataset.modified < cutoff_date:
            if dry_run:
                logging.info("[Dry-run] Would delete %s", dataset.dataset_id)
            else:
                logging.info("Deleting %s...", dataset.dataset_id)
                client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)
        else:
            logging.info(
                "Skipping %s because it was created too recently.", dataset.dataset_id
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(args.dry_run)
