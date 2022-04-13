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
"""Controller for updating raw state data latest views in BQ."""

import logging
from typing import Optional

from opencensus.stats import aggregation, measure
from opencensus.stats import view as opencensus_view

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestRawDataTableLatestView,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
)
from recidiviz.utils import monitoring
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)

m_failed_latest_views_update = measure.MeasureInt(
    "ingest/direct/controllers/direct_ingest_raw_data_table_latest_view_updater/update_views_for_state_failure",
    "Counted every time updating views for state fails",
    "1",
)

failed_latest_view_updates_view = opencensus_view.View(
    "ingest/direct/controllers/direct_ingest_raw_data_table_latest_view_updater/num_update_views_for_state_failure",
    "The sum of times a view failed to update",
    [monitoring.TagKey.CREATE_UPDATE_RAW_DATA_LATEST_VIEWS_FILE_TAG],
    m_failed_latest_views_update,
    aggregation.SumAggregation(),
)

monitoring.register_views([failed_latest_view_updates_view])


class DirectIngestRawDataTableLatestViewUpdater:
    """Controller for updating raw state data latest views in BQ."""

    def __init__(
        self,
        state_code: str,
        project_id: str,
        bq_client: BigQueryClient,
        raw_tables_sandbox_dataset_prefix: Optional[str],
        views_sandbox_dataset_prefix: Optional[str],
        dry_run: bool = False,
    ):
        self.state_code = state_code
        self.project_id = project_id
        self.bq_client = bq_client
        self.dry_run = dry_run
        self.raw_file_region_config = DirectIngestRegionRawFileConfig(state_code)
        # Sandbox prefix for dataset to read raw data files from
        self.raw_tables_sandbox_dataset_prefix = raw_tables_sandbox_dataset_prefix
        # Sandbox prefix to write raw data files to
        self.views_sandbox_dataset_prefix = views_sandbox_dataset_prefix

    def _latest_view_for_config(
        self,
        src_raw_tables_dataset: str,
        raw_file_config: DirectIngestRawFileConfig,
    ) -> Optional[DirectIngestRawDataTableLatestView]:
        """Returns the raw latest view information from the provided config, if it can
        be created or updated in BigQuery. Otherwise, returns None.
        """
        logging.info(
            "===================== CREATING VIEW FOR %s  =======================",
            raw_file_config.file_tag,
        )

        if not self.bq_client.table_exists(
            self.bq_client.dataset_ref_for_id(src_raw_tables_dataset),
            raw_file_config.file_tag,
        ):
            logging.warning(
                "Table with name [%s] does not exist in BQ... Skipping latest view update/creation",
                raw_file_config.file_tag,
            )
            return None

        if not raw_file_config.primary_key_cols:
            if self.dry_run:
                logging.info(
                    "[DRY RUN] would have skipped table named %s with empty primary key list",
                    raw_file_config.file_tag,
                )
            else:
                logging.warning(
                    "Table config with name %s has empty primary key list... Skipping "
                    "update/creation.",
                    raw_file_config.file_tag,
                )
            return None
        if raw_file_config.is_undocumented:
            if self.dry_run:
                logging.info(
                    "[DRY RUN] would have skipped undocumented table named %s",
                    raw_file_config.file_tag,
                )
            else:
                logging.warning(
                    "Table config with name %s is undocumented... Skipping "
                    "update/creation.",
                    raw_file_config.file_tag,
                )
            return None

        builder = DirectIngestRawDataTableLatestViewBuilder(
            region_code=self.state_code, raw_file_config=raw_file_config
        )

        return builder.build(
            address_overrides=address_overrides_for_view_builders(
                view_dataset_override_prefix=self.views_sandbox_dataset_prefix,
                view_builders=[builder],
            )
            if self.views_sandbox_dataset_prefix
            else None,
        )

    def _create_or_update_view(
        self, latest_view: DirectIngestRawDataTableLatestView
    ) -> None:
        if self.dry_run:
            logging.info(
                "[DRY RUN] would have created/updated view %s with query:\n %s",
                latest_view.view_id,
                latest_view.view_query,
            )
            return

        self.bq_client.create_or_update_view(view=latest_view)
        logging.info("Created/Updated view %s", latest_view.view_id)

    def update_views_for_state(self) -> None:
        """Create or update the up to date views dataset for a state with latest views"""
        src_raw_tables_dataset = raw_tables_dataset_for_region(
            self.state_code,
            sandbox_dataset_prefix=self.raw_tables_sandbox_dataset_prefix,
        )
        succeeded_tables = []
        failed_tables = []

        latest_views_to_update = []
        view_datasets = set()
        for raw_file_config in self.raw_file_region_config.raw_file_configs.values():
            view = self._latest_view_for_config(
                src_raw_tables_dataset=src_raw_tables_dataset,
                raw_file_config=raw_file_config,
            )
            if view:
                latest_views_to_update.append(view)
                view_datasets.add(view.address.dataset_id)

        standard_latest_views_dataset = raw_latest_views_dataset_for_region(
            self.state_code, sandbox_dataset_prefix=None
        )
        for view_dataset in view_datasets:
            default_table_expiration_ms = (
                TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
                if view_dataset != standard_latest_views_dataset
                else None
            )
            self.bq_client.create_dataset_if_necessary(
                self.bq_client.dataset_ref_for_id(view_dataset),
                default_table_expiration_ms=default_table_expiration_ms,
            )

        for latest_view in latest_views_to_update:
            raw_file_config = latest_view.raw_file_config
            try:
                self._create_or_update_view(latest_view=latest_view)
                succeeded_tables.append(raw_file_config.file_tag)
            except Exception as e:
                with monitoring.measurements(
                    {
                        monitoring.TagKey.CREATE_UPDATE_RAW_DATA_LATEST_VIEWS_FILE_TAG: raw_file_config.file_tag
                    }
                ) as measurements:
                    measurements.measure_int_put(m_failed_latest_views_update, 1)
                failed_tables.append(raw_file_config.file_tag)
                raise ValueError(
                    f"Couldn't create/update views for file [{raw_file_config.file_tag}]"
                ) from e

        logging.info("Succeeded tables %s", succeeded_tables)
        if failed_tables:
            logging.error("Failed tables %s", failed_tables)
