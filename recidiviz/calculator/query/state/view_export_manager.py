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
"""Export data from BigQuery views to JSON files in Cloud Storage."""
import logging

from google.cloud import bigquery

from recidiviz.big_query import view_manager
from recidiviz.big_query.big_query_client import BigQueryClientImpl, ExportQueryConfig

from recidiviz.calculator.query.state import view_config


def export_view_data_to_cloud_storage():
    """Exports data in BigQuery views to cloud storage buckets."""
    view_manager.create_dataset_and_update_views(view_config.VIEWS_TO_UPDATE)

    bq_client = BigQueryClientImpl()
    project_id = bq_client.project_id

    views_to_materialize = view_config.VIEWS_TO_MATERIALIZE_FOR_DASHBOARD_EXPORT

    # This step has to happen before the export because some views in views_to_export depend on the materialized
    # version of a view
    for view in views_to_materialize:
        bq_client.materialize_view_to_table(view)

    datasets_to_export = view_config.DATASETS_STATES_AND_VIEWS_TO_EXPORT.keys()

    for dataset_id in datasets_to_export:
        states_and_views_to_export = view_config.DATASETS_STATES_AND_VIEWS_TO_EXPORT.get(dataset_id)
        output_uri_template = view_config.OUTPUT_URI_TEMPLATE_FOR_DATASET_EXPORT.get(dataset_id)

        if not states_and_views_to_export or not output_uri_template:
            raise ValueError(f"Trying to export views from an unsupported dataset: {dataset_id}")

        views_to_export = [
            ExportQueryConfig.from_view_query(
                view=view,
                view_filter_clause=f" WHERE state_code = '{state_code}'",
                intermediate_table_name=f"{view.view_id}_table_{state_code}",
                output_uri=output_uri_template.format(
                    project_id=project_id,
                    state_code=state_code,
                    view_id=view.view_id
                ),
                output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
            for state_code, views in states_and_views_to_export.items()
            for view in views
        ]

        bq_client.export_query_results_to_cloud_storage(views_to_export)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    export_view_data_to_cloud_storage()
