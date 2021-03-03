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
"""Dashboard views related to reincarcerations."""
from typing import List

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.reincarcerations.average_days_at_liberty_by_month import (
    AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.reincarcerations.reincarceration_rate_by_stay_length import (
    REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.reincarcerations.reincarcerations_by_month import (
    REINCARCERATIONS_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.reincarcerations.reincarcerations_by_period import (
    REINCARCERATIONS_BY_PERIOD_VIEW_BUILDER,
)

REINCARCERATIONS_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_BUILDER,
    REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_BUILDER,
    REINCARCERATIONS_BY_MONTH_VIEW_BUILDER,
    REINCARCERATIONS_BY_PERIOD_VIEW_BUILDER,
]
