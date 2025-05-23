# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Dashboard views related to vitals."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_summaries import (
    VITALS_SUMMARIES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_time_series import (
    VITALS_TIME_SERIES_VIEW_BUILDER,
)

VITALS_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    VITALS_SUMMARIES_VIEW_BUILDER,
    VITALS_TIME_SERIES_VIEW_BUILDER,
]
