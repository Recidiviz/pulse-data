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
"""All views that populate the data in the dashboards."""
from typing import List

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.admissions import admissions_views
from recidiviz.calculator.query.state.views.dashboard.program_evaluation import program_evaluation_views
from recidiviz.calculator.query.state.views.dashboard.reincarcerations import reincarcerations_views
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis import revocation_analysis_views
from recidiviz.calculator.query.state.views.dashboard.revocations import revocations_views
from recidiviz.calculator.query.state.views.dashboard.supervision import supervision_views


CORE_DASHBOARD_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = (
    admissions_views.ADMISSIONS_VIEW_BUILDERS +
    reincarcerations_views.REINCARCERATIONS_VIEW_BUILDERS +
    revocations_views.REVOCATIONS_VIEW_BUILDERS +
    supervision_views.SUPERVISION_VIEW_BUILDERS +
    program_evaluation_views.PROGRAM_EVALUATION_VIEW_BUILDERS
)


LANTERN_DASHBOARD_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = \
    revocation_analysis_views.REVOCATION_ANALYSIS_VIEW_BUILDERS


DASHBOARD_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = (
        CORE_DASHBOARD_VIEW_BUILDERS +
        LANTERN_DASHBOARD_VIEW_BUILDERS
)
