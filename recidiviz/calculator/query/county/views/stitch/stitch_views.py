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

"""Views used for stitch"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county.views.stitch.combined_stitch import (
    COMBINED_STITCH_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.combined_stitch_drop_overlapping import (
    COMBINED_STITCH_DROP_OVERLAPPING_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.combined_stitch_drop_overlapping_total_jail_pop import (
    COMBINED_STITCH_DROP_OVERLAPPING_TOTAL_JAIL_POP_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.incarceration_trends_stitch_subset import (
    INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.scraper_aggregated_stitch_subset import (
    SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.single_count_stitch_subset import (
    SINGLE_COUNT_STITCH_SUBSET_VIEW_BUILDER,
)
from recidiviz.calculator.query.county.views.stitch.state_aggregate_stitch_subset import (
    STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER,
)

STITCH_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    INCARCERATION_TRENDS_STITCH_SUBSET_VIEW_BUILDER,
    SCRAPER_AGGREGATED_STITCH_SUBSET_VIEW_BUILDER,
    SINGLE_COUNT_STITCH_SUBSET_VIEW_BUILDER,
    STATE_AGGREGATE_STITCH_SUBSET_VIEW_BUILDER,
    COMBINED_STITCH_VIEW_BUILDER,
    COMBINED_STITCH_DROP_OVERLAPPING_VIEW_BUILDER,
    COMBINED_STITCH_DROP_OVERLAPPING_TOTAL_JAIL_POP_VIEW_BUILDER,
]
