# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Experiments metadata view configuration."""
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.experiments_metadata.views.experiment_assignment_sessions_utils import (
    EXPERIMENT_ASSIGNMENT_SESSIONS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.experiments_metadata.views.experiment_assignments import (
    EXPERIMENT_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments_metadata.views.officer_assignments import (
    OFFICER_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments_metadata.views.person_assignments import (
    PERSON_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments_metadata.views.state_assignments import (
    STATE_ASSIGNMENTS_VIEW_BUILDER,
)

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = [
    EXPERIMENT_ASSIGNMENTS_VIEW_BUILDER,
    OFFICER_ASSIGNMENTS_VIEW_BUILDER,
    PERSON_ASSIGNMENTS_VIEW_BUILDER,
    STATE_ASSIGNMENTS_VIEW_BUILDER,
    *EXPERIMENT_ASSIGNMENT_SESSIONS_VIEW_BUILDERS,
]
