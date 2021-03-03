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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SINGLE_COUNT_AGGREGATE_VIEW_ID: str = "single_count_aggregate"

_DESCRIPTION = """
Copy single count data to a format for stitching.
"""

with open(os.path.splitext(__file__)[0] + ".sql") as fp:
    _QUERY_TEMPLATE = fp.read()

SINGLE_COUNT_STITCH_SUBSET_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id="single_count_stitch_subset",
    view_query_template=_QUERY_TEMPLATE,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
    single_count_aggregate=SINGLE_COUNT_AGGREGATE_VIEW_ID,
    description=_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SINGLE_COUNT_STITCH_SUBSET_VIEW_BUILDER.build_and_print()
