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
"""Provides utilities for updating views within a live BigQuery instance.

This can be run on-demand whenever a set of views needs to be updated. Run locally with the following command:
    python -m recidiviz.big_query.view_update_manager
        --project_id [PROJECT_ID]
        --views_to_update [state, county, validation]
        --materialized_views_only [True, False]
"""
import argparse
import logging
import sys
from typing import Dict, List

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.calculator.query.county.view_config import VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as COUNTY_VIEW_BUILDERS
from recidiviz.calculator.query.state.view_config import VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as STATE_VIEW_BUILDERS
from recidiviz.utils.params import str_to_bool
from recidiviz.validation.views.view_config import VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as VALIDATION_VIEW_BUILDERS
from recidiviz.utils.environment import GAE_PROJECT_STAGING, GAE_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Dict[str, Dict[str, List[BigQueryViewBuilder]]] = {
    'county': COUNTY_VIEW_BUILDERS,
    'state': STATE_VIEW_BUILDERS,
    'validation': VALIDATION_VIEW_BUILDERS
}


def create_dataset_and_update_views_for_view_builders(
        view_builders_to_update: Dict[str, List[BigQueryViewBuilder]],
        materialized_views_only: bool = False):
    """Converts the map of dataset_ids to BigQueryViewBuilders lists into a map of dataset_ids to BigQueryViews by
    building each of the views. Then, calls create_dataset_and_update_views with those views and their parent
    datasets. If materialized_views_only is True, will only update views that have a set materialized_view_table_id
    field."""
    # Convert the map of dataset_ids to BigQueryViewBuilders into a map of dataset_ids to BigQueryViews by building
    # each of the views
    views_to_update: Dict[str, List[BigQueryView]] = {
        dataset: [
            view_builder.build() for view_builder in view_builders
            if not materialized_views_only or view_builder.build().materialized_view_table_id is not None
        ]
        for dataset, view_builders in view_builders_to_update.items()
    }

    _create_dataset_and_update_views(views_to_update)


def _create_dataset_and_update_views(views_to_update: Dict[str, List[BigQueryView]]):
    """Create and update the given views and their parent datasets.

    For each dataset key in the given dictionary, creates the dataset if it does not exist, and creates or updates the
    underlying views mapped to that dataset.

    If a view has a set materialized_view_table_id field, materializes the view into a table.

    Args:
        views_to_update: Dict of BigQuery dataset name to list of view objects to be created or updated.
    """
    bq_client = BigQueryClientImpl()
    for dataset_name, view_list in views_to_update.items():
        views_dataset_ref = bq_client.dataset_ref_for_id(dataset_name)
        bq_client.create_dataset_if_necessary(views_dataset_ref)

        for view in view_list:
            bq_client.create_or_update_view(views_dataset_ref, view)

            if view.materialized_view_table_id:
                bq_client.materialize_view_to_table(view)


def parse_arguments(argv):
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=[GAE_PROJECT_STAGING, GAE_PROJECT_PRODUCTION],
                        required=True)

    parser.add_argument('--views_to_update',
                        dest='views_to_update',
                        type=str,
                        choices=VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE.keys(),
                        required=True)

    parser.add_argument('--materialized_views_only',
                        dest='materialized_views_only',
                        type=str_to_bool,
                        default=False)

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    view_builders = VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE.get(known_args.views_to_update)

    if not view_builders:
        raise ValueError("Unsupported views_to_update parameter. Fix the parser to only allow supported values.")

    if known_args.materialized_views_only:
        logging.info("Limiting update to materialized views only.")

    with local_project_id_override(known_args.project_id):
        create_dataset_and_update_views_for_view_builders(view_builders, known_args.materialized_views_only)
