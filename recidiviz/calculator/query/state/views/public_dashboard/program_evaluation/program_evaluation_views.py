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
"""Public Dashboard views related to program evaluation."""

from typing import List

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state.views.public_dashboard.program_evaluation.us_nd.active_program_participation_by_region import (
    ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_BUILDER,
)

PROGRAM_EVALUATION_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_BUILDER
]
