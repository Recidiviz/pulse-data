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

This can be run on-demand whenever a set of views needs to be updated, e.g. from a local machine with proper Google
Cloud authorization initialized. If running locally, be sure to have GOOGLE_CLOUD_PROJECT set as an environment
variable.
"""

from typing import Dict, List

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView


def create_dataset_and_update_views(views_to_update: Dict[str, List[BigQueryView]]):
    """Create and update the given views and their parent datasets.

    For each dataset key in the given dictionary, creates the dataset if it does not exist, and creates or updates the
    underlying views mapped to that dataset.

    Args:
        views_to_update: Dict of BigQuery dataset name to list of view objects to be created or updated.
    """
    bq_client = BigQueryClientImpl()
    for dataset_name, view_list in views_to_update.items():
        views_dataset_ref = bq_client.dataset_ref_for_id(dataset_name)
        bq_client.create_dataset_if_necessary(views_dataset_ref)

        for view in view_list:
            bq_client.create_or_update_view(views_dataset_ref, view)


if __name__ == '__main__':
    # To run this locally, replace the argument below with a some dictionary mapping datasets to views, e.g.:
    # create_dataset_and_update_views(view_config.VIEWS_TO_UPDATE)
    create_dataset_and_update_views({})
