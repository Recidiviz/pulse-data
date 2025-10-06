# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Find views that reference raw data tables/views directly."""

from collections import defaultdict
from typing import Dict, List, Set

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_helpers import get_raw_data_table_and_view_datasets
from recidiviz.ingest.direct.views.direct_ingest_all_view_collector import (
    RAW_DATA_ALL_VIEW_ID_SUFFIX,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    RAW_DATA_LATEST_VIEW_ID_SUFFIX,
)
from recidiviz.validation.views.state.raw_data.stale_raw_data_validation import (
    collect_stale_raw_data_view_builders,
)


def find_direct_raw_data_references(
    view_builders: List[BigQueryViewBuilder],
) -> Dict[StateCode, Dict[str, Set[BigQueryAddress]]]:
    """
    Finds direct references to raw data tables in the given list of BigQuery view builders.

    A direct reference is identified when a view queries the raw data we receive from a state, rather than querying the state-agnostic format.

    The returned dictionary has the following structure:
    {
        StateCode: {
            FileTag: Set[BigQueryAddress]
        }
    }

    - StateCode: Represents the state code of the source raw dataset.
    - FileTag: Represents the table id of the raw data table. Note that there is no distinction made for latest views.
               Ex, myFileTag_latest and myFileTag are both represented as myFileTag.
    - BigQueryAddress: Represents the address of the BigQuery view that references the raw data table.

    For example, a returned dictionary entry might look like this:
    {
        StateCode.US_MO: {
            'LBAKRDTA_TAK033': {BigQueryAddress(dataset_id='analyst_data', table_id='us_mo_restrictive_housing_record')}
        }
    }
    Note that the same view address may be included multiple times if it references multiple raw data tables.
    """
    raw_data_references: Dict[StateCode, Dict[str, Set[BigQueryAddress]]] = defaultdict(
        lambda: defaultdict(set)
    )
    # Exempt views that are used for raw data validation
    raw_data_validation_views = set(
        builder.address for builder in collect_stale_raw_data_view_builders()
    )
    views = [
        builder.build(sandbox_context=None)
        for builder in view_builders
        if builder.address not in raw_data_validation_views
    ]
    raw_datasets = get_raw_data_table_and_view_datasets()
    for view in views:
        for parent_table in view.parent_tables:
            # We don't count raw data references that are *in* raw data datasets
            if view.dataset_id in raw_datasets:
                continue

            if parent_table.dataset_id in raw_datasets:
                file_tag = parent_table.table_id.removesuffix(
                    RAW_DATA_LATEST_VIEW_ID_SUFFIX
                ).removesuffix(RAW_DATA_ALL_VIEW_ID_SUFFIX)
                state_code = raw_datasets[parent_table.dataset_id]
                raw_data_references[state_code][file_tag].add(view.address)
    return raw_data_references
