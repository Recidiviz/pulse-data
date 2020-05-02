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
"""Export data from BigQuery to JSON files in Cloud Storage."""
import logging
from typing import List

from google.cloud import bigquery

from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_manager, view_config, \
    dashboard_export_config
from recidiviz.utils import metadata


def export_dashboard_data_to_cloud_storage(bucket: str):
    """Exports data needed by the dashboard to the cloud storage bucket.

    This is a two-step process. First, for each view, the view query is executed
    and the entire result is loaded into a table in BigQuery. Then, for each
    table, the contents are exported to the cloud storage bucket in JSON format.
    This has to be a two-step process because BigQuery doesn't support exporting
    a view directly, it must be materialized in a table first.

    Args:
        bucket: The cloud storage location where the exported data should go.
    """
    view_manager.create_dataset_and_update_views(view_manager.VIEWS_TO_UPDATE)

    dataset_ref = bq_utils.client().dataset(view_config.DASHBOARD_VIEWS_DATASET)
    views_to_export = dashboard_export_config.VIEWS_TO_EXPORT

    _export_views_to_tables(dataset_ref, views_to_export)

    _export_view_tables_to_cloud_storage(dataset_ref, views_to_export, bucket)


def _export_views_to_tables(dataset_ref: bigquery.dataset.DatasetReference,
                            views_to_export: List[bqview.BigQueryView]):
    for state in dashboard_export_config.STATES_TO_EXPORT:
        for view in views_to_export:
            query = "SELECT * FROM `{project_id}.{dataset}.{table}`" \
                        "WHERE state_code = '{state_code}'"\
                    .format(project_id=metadata.project_id(),
                            dataset=dataset_ref.dataset_id,
                            table=view.view_id,
                            state_code=state)
            bq_utils.create_or_update_table_from_view(
                dataset_ref, view, query, _table_name_for_view(view, state))


def _export_view_tables_to_cloud_storage(
        dataset_ref: bigquery.dataset.DatasetReference, views_to_export:
        List[bqview.BigQueryView], bucket: str):
    for state in dashboard_export_config.STATES_TO_EXPORT:
        for view in views_to_export:
            bq_utils.export_to_cloud_storage(
                dataset_ref, bucket, _table_name_for_view(view, state), _destination_filename_for_view(view, state))


def _table_name_for_view(view: bqview.BigQueryView,
                         state_code: str) -> str:
    """Returns the name of the table where the view's contents are."""
    return view.view_id + '_table_' + state_code


def _destination_filename_for_view(view: bqview.BigQueryView,
                                   state_code: str) -> str:
    """Returns the filename that should be used as an export destination."""
    return state_code + '/' + view.view_id + '.json'


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    export_dashboard_data_to_cloud_storage('ENTER-BUCKET-HERE')
