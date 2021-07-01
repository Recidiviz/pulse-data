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
"""Provides utilities for updating views within a live BigQuery instance."""
from typing import Dict, Set

from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker


def get_managed_views_for_dataset_map(
    managed_views_dag_walker: BigQueryViewDagWalker,
) -> Dict[str, Set[BigQueryAddress]]:
    """Creates a dictionary mapping every managed dataset in BigQuery to the set
    of managed views in the dataset. Returned dictionary's key is a dataset_id and
    each key's value is a set of all the BigQueryAddress's that are from the referenced dataset.
    """
    managed_views_for_dataset_map: Dict[str, Set[BigQueryAddress]] = {}
    for view in managed_views_dag_walker.views:
        managed_views_for_dataset_map.setdefault(view.address.dataset_id, set()).add(
            view.address
        )
    return managed_views_for_dataset_map
