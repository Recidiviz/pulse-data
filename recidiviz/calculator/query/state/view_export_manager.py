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
"""Export data from BigQuery views to JSON files in Cloud Storage.

Run this export locally with the following command:
    python -m recidiviz.calculator.query.state.view_export_manager --project_id [PROJECT_ID]
"""
import argparse
import logging
import sys

from google.cloud import bigquery

from recidiviz.big_query import view_manager
from recidiviz.big_query.big_query_client import BigQueryClientImpl, ExportQueryConfig

from recidiviz.calculator.query.state import view_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING, GAE_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override


def export_view_data_to_cloud_storage():
    """Exports data in BigQuery views to cloud storage buckets."""
    view_builders_for_views_to_update = view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE
    view_manager.create_dataset_and_update_views_for_view_builders(view_builders_for_views_to_update)

    bq_client = BigQueryClientImpl()
    project_id = bq_client.project_id

    view_builders_to_materialize = view_config.VIEW_BUILDERS_FOR_VIEWS_TO_MATERIALIZE_FOR_DASHBOARD_EXPORT

    # This step has to happen before the export because some views in views_to_export depend on the materialized
    # version of a view
    for view_builder in view_builders_to_materialize:
        view = view_builder.build()
        bq_client.materialize_view_to_table(view)

    datasets_to_export = view_config.DATASETS_STATES_AND_VIEW_BUILDERS_TO_EXPORT.keys()

    for dataset_id in datasets_to_export:
        states_and_view_builders_to_export = view_config.DATASETS_STATES_AND_VIEW_BUILDERS_TO_EXPORT.get(dataset_id)
        output_uri_template = view_config.OUTPUT_URI_TEMPLATE_FOR_DATASET_EXPORT.get(dataset_id)

        if not states_and_view_builders_to_export or not output_uri_template:
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
            for state_code, view_builders in states_and_view_builders_to_export.items()
            for view in [view_builder.build() for view_builder in view_builders]
        ]

        bq_client.export_query_results_to_cloud_storage(views_to_export)


def parse_arguments(argv):
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=[GAE_PROJECT_STAGING, GAE_PROJECT_PRODUCTION],
                        required=True)

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        export_view_data_to_cloud_storage()
