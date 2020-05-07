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
"""Dashboard export configuration."""

from typing import List

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state.view_config import VIEWS_TO_UPDATE
from recidiviz.calculator.query.state.views.reference import reference_views
from recidiviz.calculator.query.state.views.reference.most_recent_job_id_by_metric_and_state_code import \
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW

STATES_TO_EXPORT = ['US_MO', 'US_ND']

VIEWS_TO_EXCLUDE_FROM_EXPORT: List[BigQueryView] = \
    reference_views.REF_VIEWS

VIEWS_TO_EXPORT = [
    view for view_list in VIEWS_TO_UPDATE.values() for view in view_list
    if view not in VIEWS_TO_EXCLUDE_FROM_EXPORT
]

VIEWS_TO_MATERIALIZE_FOR_DASHBOARD_EXPORT = [
    MOST_RECENT_JOB_ID_BY_METRIC_AND_STATE_CODE_VIEW
]
