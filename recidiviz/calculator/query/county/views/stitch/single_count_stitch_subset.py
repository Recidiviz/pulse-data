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
"""Single count data used for stitch"""

import os
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.export_config import \
    COUNTY_BASE_TABLES_BQ_DATASET
from recidiviz.utils import metadata

PROJECT_ID: str = metadata.project_id()
VIEWS_DATASET: str = COUNTY_BASE_TABLES_BQ_DATASET
SINGLE_COUNT_AGGREGATE_VIEW_ID: str = 'single_count_aggregate'

_DESCRIPTION = """
Copy single count data to a format for stitching.
"""

with open(os.path.splitext(__file__)[0] + '.sql') as fp:
    _QUERY = fp.read().format(
        project_id=PROJECT_ID, views_dataset=VIEWS_DATASET,
        single_count_aggregate=SINGLE_COUNT_AGGREGATE_VIEW_ID,
        description=_DESCRIPTION)

SINGLE_COUNT_STITCH_SUBSET_VIEW = BigQueryView(
    view_id='single_count_stitch_subset',
    view_query=_QUERY
)

if __name__ == '__main__':
    print(SINGLE_COUNT_STITCH_SUBSET_VIEW.view_query)
