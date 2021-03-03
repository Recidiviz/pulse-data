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
"""List all state aggregate views."""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county.views.state_aggregates import (
    combined_state_aggregate,
)
from recidiviz.calculator.query.county.views.state_aggregates import (
    state_aggregate_collapsed_to_fips,
)

STATE_AGGREGATE_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    combined_state_aggregate.COMBINED_STATE_AGGREGATE_VIEW_BUILDER,
    state_aggregate_collapsed_to_fips.STATE_AGGREGATES_COLLAPSED_TO_FIPS_BUILDER,
]
