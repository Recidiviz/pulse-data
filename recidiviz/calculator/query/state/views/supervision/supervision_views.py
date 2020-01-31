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
"""Views related to supervision."""
from recidiviz.calculator.query.state.views.supervision.supervision_termination_by_type_by_period import \
    SUPERVISION_TERMINATION_BY_TYPE_BY_PERIOD_VIEW
from recidiviz.calculator.query.state.views.supervision.us_nd.average_change_lsir_score_by_month import \
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW
from recidiviz.calculator.query.state.views.supervision.supervision_termination_by_type_by_month import \
    SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW
from recidiviz.calculator.query.state.views.supervision.us_nd.average_change_lsir_score_by_period import \
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW

SUPERVISION_VIEWS = [
    AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW,
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW,
    SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW,
    SUPERVISION_TERMINATION_BY_TYPE_BY_PERIOD_VIEW,
]
