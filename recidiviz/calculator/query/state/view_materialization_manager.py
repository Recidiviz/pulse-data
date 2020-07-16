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
"""Materializes views into tables. Views must have a set materialized_view_table_id in order to be materialized.

Run this locally with the following command:
    python -m recidiviz.calculator.query.state.view_materialization_manager --project_id [PROJECT_ID]
"""
import argparse
import logging
import sys
from typing import List

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryViewBuilder

from recidiviz.calculator.query.state import view_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING, GAE_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override


def materialize_views(view_builders_to_materialize: List[BigQueryViewBuilder]):
    """Materializes views in VIEW_BUILDERS_FOR_VIEWS_TO_MATERIALIZE_FOR_DASHBOARD_EXPORT into tables."""
    bq_client = BigQueryClientImpl()

    for view_builder in view_builders_to_materialize:
        view = view_builder.build()
        bq_client.materialize_view_to_table(view)


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

    # Change this variable to materialize a different set of views. Views must have a set materialized_view_table_id in
    # order to be materialized.
    views_to_materialize = view_config.VIEW_BUILDERS_FOR_VIEWS_TO_MATERIALIZE_FOR_DASHBOARD_EXPORT

    with local_project_id_override(known_args.project_id):
        materialize_views(views_to_materialize)
