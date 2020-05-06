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
# pylint: disable=line-too-long
"""Define views for combining scraper & state-reports & ITP."""

import os

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.county import view_config
from recidiviz.calculator.query.county.views.stitch import combined_stitch
from recidiviz.calculator.query.county.views.stitch.incarceration_trends_stitch_subset \
    import INCARCERATION_TRENDS_STITCH_SUBSET_VIEW
from recidiviz.calculator.query.county.views.stitch.scraper_aggregated_stitch_subset \
    import SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW
from recidiviz.calculator.query.county.views.stitch.single_count_stitch_subset \
    import SINGLE_COUNT_STITCH_SUBSET_VIEW
from recidiviz.calculator.query.county.views.stitch.state_aggregate_stitch_subset \
    import STATE_AGGREGATE_STITCH_SUBSET_VIEW
_DESCRIPTION = """ Combine {itp}, {state}, {scraper}, and {single_count} into
one unified view, limited to total jail population only. When overlapping
data exists, we select {state} data first. We then select any {itp}
data that exists before {state} data. We then select any {scraper}
data that exists after {state} data and finally the {single_count}
data after that.

Note: we use valid_from to check cutoffs, instead of checking
valid_from and valid_to (eg: {itp}.valid_to < {state}.valid_from).
This is because all data points are plotted using valid_from.  """.format(
    state=INCARCERATION_TRENDS_STITCH_SUBSET_VIEW.view_id,
    scraper=SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW.view_id,
    single_count=SINGLE_COUNT_STITCH_SUBSET_VIEW.view_id,
    itp=STATE_AGGREGATE_STITCH_SUBSET_VIEW.view_id)

with open(os.path.splitext(__file__)[0] + '.sql') as fp:
    _QUERY_TEMPLATE = fp.read()

COMBINED_STITCH_DROP_OVERLAPPING_TOTAL_JAIL_POP_VIEW = BigQueryView(
    dataset_id=view_config.VIEWS_DATASET,
    view_id='combined_stitch_drop_overlapping_total_jail_pop',
    view_query_template=_QUERY_TEMPLATE,
    views_dataset=view_config.VIEWS_DATASET,
    combined_stitch=combined_stitch.COMBINED_STITCH_VIEW.view_id,
    description=_DESCRIPTION
)

if __name__ == '__main__':
    print(COMBINED_STITCH_DROP_OVERLAPPING_TOTAL_JAIL_POP_VIEW.view_query)
