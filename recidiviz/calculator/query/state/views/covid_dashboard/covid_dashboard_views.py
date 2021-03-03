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
"""All views that populate the public data in the COVID dashboard."""
from typing import List

from recidiviz.calculator.query.state.views.covid_dashboard.facility_case_data import (
    FACILITY_CASE_DATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.covid_dashboard.facility_metadata import (
    FACILITY_METADATA_VIEW_BUILDER,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder


COVID_DASHBOARD_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    FACILITY_CASE_DATA_VIEW_BUILDER,
    FACILITY_METADATA_VIEW_BUILDER,
]
