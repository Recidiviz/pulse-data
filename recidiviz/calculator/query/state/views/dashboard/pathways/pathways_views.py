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
"""Dashboard views related to pathways."""
from typing import List

from recidiviz.calculator.query.state.views.dashboard.pathways.prison_population_snapshot_by_dimension import (
    PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.prison_population_snapshot_person_level import (
    PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.prison_population_time_series import (
    PRISON_POPULATION_TIME_SERIES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.supervision_to_liberty_count_by_month import (
    SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.supervision_to_prison_count_by_month import (
    SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder

PATHWAYS_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER,
    PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_PERSON_LEVEL_VIEW_BUILDER,
    PRISON_POPULATION_TIME_SERIES_VIEW_BUILDER,
    SUPERVISION_TO_LIBERTY_COUNT_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_TO_PRISON_COUNT_BY_MONTH_VIEW_BUILDER,
]
