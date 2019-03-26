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

"""Create and update views and their parent dataset.

If views are derived from other views, they must be listed after the views
they rely on in VIEWS_TO_UPDATE.
"""

from typing import List

from recidiviz.calculator.bq import bq_utils
from recidiviz.calculator.bq.views import bqview
from recidiviz.calculator.bq.views import view_config

from recidiviz.calculator.bq.views import view_queries
from recidiviz.calculator.bq.views.bonds import bond_views
from recidiviz.calculator.bq.views.charges import charge_views
from recidiviz.calculator.bq.views.combined_aggregates import \
    combined_aggregate_views
from recidiviz.calculator.bq.views.population import population_views
from recidiviz.calculator.bq.views.state_aggregates import state_aggregate_views


VIEWS_TO_UPDATE: List[bqview.BigQueryView] = [
    view_queries.PERSON_COUNT_VIEW,
    state_aggregate_views.STATE_AGGREGATE_VIEW,
    combined_aggregate_views.INTERPOLATED_STATE_AGGREGATE_VIEW,
    combined_aggregate_views.SCRAPER_DATA_AGGREGATED,
    combined_aggregate_views.SCRAPER_AND_STATE_COMBINED
] + bond_views.BOND_VIEWS + \
    charge_views.CHARGE_VIEWS + \
    population_views.POPULATION_VIEWS


def create_dataset_and_update_views(
        dataset_name: str,
        views_to_update: List[bqview.BigQueryView]):
    """Create and update Views and their parent Dataset.

    Create a parent Views dataset if it does not exist, and
    creates or updates the underlying Views as defined in
    recidiviz.calculator.bq.views.bqview

    Args:
        dataset_name: Name of BigQuery dataset to contain Views. Gets created
            if it does not already exist.
        views_to_update: View objects to be created or updated.
            Should be VIEWS_TO_UPDATE defined at top of view_manager.py
    """
    views_dataset_ref = bq_utils.client().dataset(dataset_name)
    bq_utils.create_dataset_if_necessary(views_dataset_ref)

    for view in views_to_update:
        bq_utils.create_or_update_view(views_dataset_ref, view)


if __name__ == '__main__':
    create_dataset_and_update_views(view_config.VIEWS_DATASET, VIEWS_TO_UPDATE)
