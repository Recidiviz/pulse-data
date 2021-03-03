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
"""Dashboard views related to admissions."""
from typing import List

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.admissions.admissions_by_type_by_period import (
    ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.admissions.admissions_by_type_by_month import (
    ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.admissions.admissions_versus_releases_by_month import (
    ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.admissions.admissions_versus_releases_by_period import (
    ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_BUILDER,
)

ADMISSIONS_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_BUILDER,
    ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_BUILDER,
    ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER,
    ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_BUILDER,
]
