# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Views related to the impact dashboard."""
from typing import List

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.impact.us_tn_compliant_reporting_workflows_impact import (
    US_TN_COMPLIANT_REPORTING_WORKFLOWS_IMPACT_VIEW_BUILDER,
)

# list all view builders needed for the impact dashboard
IMPACT_DASHBOARD_VIEW_BUILDERS: List[SelectedColumnsBigQueryViewBuilder] = [
    US_TN_COMPLIANT_REPORTING_WORKFLOWS_IMPACT_VIEW_BUILDER,
]
