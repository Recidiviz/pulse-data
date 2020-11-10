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

"""Exports data from the state dataset in BigQuery to another dataset in BigQuery.


Example usage (run from `pipenv shell`):

python -m recidiviz.export.state.state_bq_table_export_to_dataset --project-id recidiviz-staging \
--dry-run True --target-dataset my-test --state-codes US_ND US_MO
"""


import argparse
import logging
from typing import List

from recidiviz.big_query.big_query_client import BigQueryClientImpl, BigQueryClient
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.export.state.state_bq_table_export_utils import state_table_export_query_str

from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def copy_table_to_dataset(target_dataset: str,
                          target_table: str,
                          export_query: str,
                          bq_client: BigQueryClient) -> None:
    """Copies the results of the given query to the target table and dataset, overwriting what lives there if the
    table already exists."""
    bq_client.create_table_from_query_async(target_dataset, target_table, export_query, [], True)


def run_export(project_id: str,
               dry_run: bool,
               state_codes: List[str],
               target_dataset: str) -> None:
    """Performs the export operation, exporting rows for the given state codes from the tables from the state dataset
    in the given project to tables with the same names in the target dataset."""
    big_query_client = BigQueryClientImpl()
    dataset_ref = big_query_client.dataset_ref_for_id(STATE_BASE_DATASET)
    if not big_query_client.dataset_exists(dataset_ref):
        raise ValueError(f'Dataset {dataset_ref.dataset_id} does not exist')

    tables = big_query_client.list_tables(dataset_ref.dataset_id)

    for table in tables:
        logging.info("******************************")
        export_query = state_table_export_query_str(table, state_codes)
        logging.info(export_query)

        if not export_query:
            continue

        if target_dataset:
            if dry_run:
                logging.info("[DRY RUN] Exporting to target project.dataset.table [%s.%s.%s]",
                             project_id, target_dataset, table.table_id)
            else:
                logging.info("Exporting to target project.dataset.table [%s.%s.%s]",
                             project_id, target_dataset, table.table_id)
                copy_table_to_dataset(target_dataset, table.table_id, export_query, big_query_client)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--project-id', required=True,
                        help='The id for this particular project, E.g. \'recidiviz-123\'')

    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs script in dry-run mode, only prints the operations it would perform.')

    parser.add_argument('--target-dataset', required=True, help='The target BigQuery dataset to export data to')

    parser.add_argument('--state-codes', required=True, nargs='+', help='The state codes to export data for')

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    with local_project_id_override(args.project_id):
        run_export(args.project_id, args.dry_run, args.state_codes, args.target_dataset)
