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
from recidiviz.calculator.query.state.views.dashboard.admissions import admissions_views
from recidiviz.calculator.query.state.views.dashboard.program_evaluation import program_evaluation_views
from recidiviz.calculator.query.state.views.dashboard.reincarcerations import reincarcerations_views
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis import revocation_analysis_views
from recidiviz.calculator.query.state.views.dashboard.revocations import revocations_views
from recidiviz.calculator.query.state.views.dashboard.supervision import supervision_views

DASHBOARD_VIEWS = (
    admissions_views.ADMISSIONS_VIEWS +
    reincarcerations_views.REINCARCERATIONS_VIEWS +
    revocations_views.REVOCATIONS_VIEWS +
    supervision_views.SUPERVISION_VIEWS +
    program_evaluation_views.PROGRAM_EVALUATION_VIEWS +
    revocation_analysis_views.REVOCATION_ANALYSIS_VIEWS
)
