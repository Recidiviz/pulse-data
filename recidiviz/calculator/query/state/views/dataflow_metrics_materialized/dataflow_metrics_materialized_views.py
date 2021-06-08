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
"""Materialized copies of dataflow metrics filtered to results from the most recent jobs."""


from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    MOST_RECENT_METRICS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_single_day_dataflow_metrics import (
    MOST_RECENT_SINGLE_DAY_METRICS_VIEW_BUILDERS,
)

# NOTE: These views must be listed in order of dependency. For example, if reference view Y depends on reference view X,
# then view X should appear in the list before view Y.

DATAFLOW_METRICS_MATERIALIZED_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = (
    MOST_RECENT_METRICS_VIEW_BUILDERS + MOST_RECENT_SINGLE_DAY_METRICS_VIEW_BUILDERS
)
