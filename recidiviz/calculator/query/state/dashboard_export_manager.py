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
"""Export data from BigQuery to JSON files in Cloud Storage."""

import logging

from recidiviz.big_query import view_manager
from recidiviz.big_query.big_query_client import BigQueryClientImpl, ExportViewConfig

from recidiviz.calculator.query.state import dataset_config, dashboard_export_config, view_config


def export_dashboard_data_to_cloud_storage(bucket: str):
    """Exports data needed by the dashboard to the cloud storage bucket.

    Args:
        bucket: The cloud storage location where the exported data should go.
    """
    view_manager.create_dataset_and_update_views(view_config.VIEWS_TO_UPDATE)

    bq_client = BigQueryClientImpl()
    dataset_ref = bq_client.dataset_ref_for_id(dataset_config.DASHBOARD_VIEWS_DATASET)
    views_to_export = dashboard_export_config.VIEWS_TO_EXPORT

    bq_client.export_views_to_cloud_storage(
        dataset_ref,
        [ExportViewConfig(
            view=view,
            view_filter_clause=f" WHERE state_code = '{state}'",
            intermediate_table_name=f"{view.view_id}_table_{state}",
            output_uri=f"gs://{bucket}/{state}/{view.view_id}.json")
         for state in dashboard_export_config.STATES_TO_EXPORT
         for view in views_to_export])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    export_dashboard_data_to_cloud_storage('ENTER-BUCKET-HERE')
