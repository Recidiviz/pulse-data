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
"""Dashboard export configuration."""

from typing import List

from recidiviz.calculator.bq import bqview
from recidiviz.calculator.bq.dashboard.views import view_manager

STATES_TO_EXPORT = ['US_ND']

VIEWS_TO_EXCLUDE_FROM_EXPORT: List[bqview.BigQueryView] = \
    view_manager.reference_views.REF_VIEWS

# Views that rely only on database tables
STANDARD_VIEWS_TO_EXPORT = [
    view for view in view_manager.VIEWS_TO_UPDATE
    if view not in VIEWS_TO_EXCLUDE_FROM_EXPORT and
    view not in view_manager.DATAFLOW_VIEWS
]

# Views that rely on metrics from Dataflow jobs
DATAFLOW_VIEWS_TO_EXPORT = [
    view for view in view_manager.VIEWS_TO_UPDATE
    if view not in VIEWS_TO_EXCLUDE_FROM_EXPORT and
    view in view_manager.DATAFLOW_VIEWS
]
