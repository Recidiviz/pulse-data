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
from recidiviz.ingest.direct.controllers.direct_ingest_raw_data_table_latest_view_updater import \
    DirectIngestRawDataTableLatestViewUpdater
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def main(state_code: str, project_id: str, dry_run: bool):
    bq_client = BigQueryClientImpl(project_id=project_id)
    updater = DirectIngestRawDataTableLatestViewUpdater(state_code, project_id, bq_client, dry_run)
    updater.update_views_for_state()


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
