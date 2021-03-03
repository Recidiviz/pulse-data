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
"""Dashboard views related to supervision."""
from typing import List

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.average_change_lsir_score_by_period import (
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.average_change_lsir_score_by_month import (
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.case_terminations_by_type_by_month import (
    CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.case_terminations_by_type_by_officer_by_period import (
    CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.supervision.supervision_termination_by_type_by_period import (
    SUPERVISION_TERMINATION_BY_TYPE_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.supervision.supervision_termination_by_type_by_month import (
    SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_BUILDER,
)

SUPERVISION_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_BUILDER,
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_BUILDER,
    CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_BUILDER,
    CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_BUILDER,
    SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_TERMINATION_BY_TYPE_BY_PERIOD_VIEW_BUILDER,
]
