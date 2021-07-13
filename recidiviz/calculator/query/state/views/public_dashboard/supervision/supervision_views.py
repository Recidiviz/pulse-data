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
"""Public dashboard views related to supervision."""

from typing import List

from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_population_by_district_by_demographics import (
    SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_population_by_month_by_demographics import (
    SUPERVISION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_population_by_prioritized_race_and_ethnicity_by_period import (
    SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_revocations_by_period_by_type_by_demographics import (
    SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_success_by_month import (
    SUPERVISION_SUCCESS_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_success_by_period_by_demographics import (
    SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_terminations_by_month import (
    SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_terminations_by_period_by_demographics import (
    SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder

SUPERVISION_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_BUILDER,
    SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_VIEW_VIEW_BUILDER,
    SUPERVISION_SUCCESS_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER,
    SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER,
]
