# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""All the views for Recidiviz user report metrics."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.user_metrics.insights_user_available_actions import (
    INSIGHTS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.officer_monthly_usage_report import (
    OFFICER_MONTHLY_USAGE_REPORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.user_metrics.workflows_user_available_actions import (
    WORKFLOWS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER,
)

USAGE_REPORTS_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    OFFICER_MONTHLY_USAGE_REPORT_VIEW_BUILDER,
    INSIGHTS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER,
    WORKFLOWS_USER_AVAILABLE_ACTIONS_VIEW_BUILDER,
]
