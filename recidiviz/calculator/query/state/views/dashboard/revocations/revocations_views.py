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
"""Dashboard views related to revocations of supervision."""
from typing import List

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.revocations.revocations_by_month import (
    REVOCATIONS_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocations.revocations_by_officer_by_period import (
    REVOCATIONS_BY_OFFICER_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocations.revocations_by_period import (
    REVOCATIONS_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocations.revocations_by_race_and_ethnicity_by_period import (
    REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocations.revocations_by_site_id_by_period import (
    REVOCATIONS_BY_SITE_ID_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocations.revocations_by_supervision_type_by_month import (
    REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocations.revocations_by_violation_type_by_month import (
    REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_BUILDER,
)

REVOCATIONS_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    REVOCATIONS_BY_MONTH_VIEW_BUILDER,
    REVOCATIONS_BY_PERIOD_VIEW_BUILDER,
    REVOCATIONS_BY_OFFICER_BY_PERIOD_VIEW_BUILDER,
    REVOCATIONS_BY_SITE_ID_BY_PERIOD_VIEW_BUILDER,
    REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER,
    REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_BUILDER,
    REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_BUILDER,
]
