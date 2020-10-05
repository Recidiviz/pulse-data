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
"""Controller for updating raw state data tables in BQ."""

import logging

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRegionRawFileConfig, \
    DirectIngestRawFileConfig
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestRawDataTableLatestView


class DirectIngestRawDataTableLatestViewUpdater:
    """Controller for updating raw state data tables in BQ."""

    def __init__(self,
                 state_code: str,
                 project_id: str,
                 bq_client: BigQueryClient,
                 dry_run: bool = False):
        self.state_code = state_code
        self.project_id = project_id
        self.bq_client = bq_client
        self.dry_run = dry_run
        self.raw_file_region_config = DirectIngestRegionRawFileConfig(state_code)

    def _create_or_update_views_for_table(
            self,
            raw_file_config: DirectIngestRawFileConfig,
            views_dataset: str):
        """Creates/Updates views corresponding to the provided |raw_file_config|."""
        logging.info('===================== CREATING QUERIES FOR %s  =======================', raw_file_config.file_tag)

        if not raw_file_config.primary_key_cols:
            if self.dry_run:
                logging.info('[DRY RUN] would have skipped table named %s with empty primary key list', raw_file_config.
                             file_tag)
            else:
                logging.warning('Table config with name %s has empty primary key list... Skipping '
                                'update/creation.', raw_file_config.file_tag)
            return

        latest_view = DirectIngestRawDataTableLatestView(
            region_code=self.state_code,
            raw_file_config=raw_file_config)

        if self.dry_run:
            logging.info('[DRY RUN] would have created/updated view %s with query:\n %s',
                         latest_view.view_id, latest_view.view_query)
            return

        views_dataset_ref = self.bq_client.dataset_ref_for_id(views_dataset)
        self.bq_client.create_or_update_view(dataset_ref=views_dataset_ref, view=latest_view)
        logging.info('Created/Updated view %s', latest_view.view_id)

    def update_tables_for_state(self):
        views_dataset = f'{self.state_code}_raw_data_up_to_date_views'
        succeeded_tables = []
        failed_tables = []
        for raw_file_config in self.raw_file_region_config.raw_file_configs.values():
            try:
                self._create_or_update_views_for_table(
                    raw_file_config=raw_file_config,
                    views_dataset=views_dataset)
                succeeded_tables.append(raw_file_config.file_tag)
            except Exception:
                failed_tables.append(raw_file_config.file_tag)
                logging.exception("Couldn't create/update views for file %s", raw_file_config.file_tag)

        logging.info('Succeeded tables %s', succeeded_tables)
        if failed_tables:
            logging.error('Failed tables %s', failed_tables)
