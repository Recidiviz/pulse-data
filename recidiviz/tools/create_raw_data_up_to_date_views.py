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
Script for creating/updating all views in the us_xx_raw_data_up_to_date_views dataset.

When in dry-run mode (default), this will only log the output rather than uploading to BQ.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.create_raw_data_up_to_date_views \
    --project-id recidiviz-staging --state-code US_ID --dry-run True

"""

import argparse
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.ingest.direct.query_utils import get_raw_tables_for_state, get_raw_table_config
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestRawDataTableLatestView
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def create_or_update_views_for_table(
        raw_table_name: str,
        project_id: str,
        state_code: str,
        views_dataset: str,
        dry_run: bool):
    """Creates/Updates views corresponding to the provided |raw_table_name|."""
    logging.info('===================== CREATING QUERIES FOR %s  =======================', raw_table_name)
    raw_table_config = get_raw_table_config(region_code=state_code,
                                            raw_table_name=raw_table_name)
    if not raw_table_config.primary_key_cols:
        if dry_run:
            logging.info('[DRY RUN] would have skipped table named %s with empty primary key list', raw_table_name)
        else:
            logging.warning('Table config with name %s has empty primary key list... Skipping '
                            'update/creation.', raw_table_name)
        return

    latest_view = DirectIngestRawDataTableLatestView(
        region_code=state_code,
        raw_file_config=raw_table_config)

    if dry_run:
        logging.info('[DRY RUN] would have created/updated view %s with query:\n %s',
                     latest_view.view_id, latest_view.view_query)
        return

    bq_client = BigQueryClientImpl(project_id=project_id)
    views_dataset_ref = bq_client.dataset_ref_for_id(views_dataset)
    bq_client.create_or_update_view(dataset_ref=views_dataset_ref, view=latest_view)
    logging.info('Created/Updated view %s', latest_view.view_id)


def main(state_code: str, project_id: str, dry_run: bool):
    views_dataset = f'{state_code}_raw_data_up_to_date_views'
    succeeded_tables = []
    failed_tables = []
    for raw_table_name in get_raw_tables_for_state(state_code):
        try:
            create_or_update_views_for_table(
                state_code=state_code,
                raw_table_name=raw_table_name,
                project_id=project_id,
                views_dataset=views_dataset,
                dry_run=dry_run)
            succeeded_tables.append(raw_table_name)
        except Exception:
            failed_tables.append(raw_table_name)
            logging.exception("Couldn't create/update views for file %s", raw_table_name)

    logging.info('Succeeded tables %s', succeeded_tables)
    if failed_tables:
        logging.error('Failed tables %s', failed_tables)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs copy in dry-run mode, only prints the file copies it would do.')
    parser.add_argument('--project-id', required=True, help='The project_id to upload views to')
    parser.add_argument('--state-code', required=True, help='The state to upload views for.')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    with local_project_id_override(args.project_id):
        main(project_id=args.project_id, state_code=args.state_code.lower(), dry_run=args.dry_run)
