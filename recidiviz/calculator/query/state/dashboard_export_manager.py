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

from recidiviz.calculator.query import bq_utils, bqview
from recidiviz.calculator.query.state import (dashboard_export_config,
                                              view_config, view_manager)
from recidiviz.utils import metadata


def export_dashboard_data_to_cloud_storage(bucket: str):
    """Exports data needed by the dashboard to the cloud storage bucket.

    Args:
        bucket: The cloud storage location where the exported data should go.
    """
    view_manager.create_dataset_and_update_views(view_manager.VIEWS_TO_UPDATE)
    dataset_ref = bq_utils.client().dataset(view_config.DASHBOARD_VIEWS_DATASET)
    views_to_export = dashboard_export_config.VIEWS_TO_EXPORT

    bq_utils.export_views_to_cloud_storage(
        dataset_ref, bucket,
        [bq_utils.ExportViewConfig(
            view=view,
            intermediate_table_query=\
            "SELECT * FROM `{project_id}.{dataset}.{table}`" \
            " WHERE state_code = '{state_code}'"\
                        .format(project_id=metadata.project_id(),
                                dataset=dataset_ref.dataset_id,
                                table=view.view_id,
                                state_code=state),
            intermediate_table_name=_table_name_for_view(view, state),
            filename=_destination_filename_for_view(view, state))
         for state in dashboard_export_config.STATES_TO_EXPORT
         for view in views_to_export])


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
