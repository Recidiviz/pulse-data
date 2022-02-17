# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Experiments view configuration."""
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.experiments.views.case_triage_feedback_actions import (
    CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.case_triage_metrics import (
    CASE_TRIAGE_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.experiments import (
    EXPERIMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.officer_assignments import (
    OFFICER_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.officer_attributes import (
    OFFICER_ATTRIBUTES_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.person_assignments import (
    PERSON_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.person_attributes import (
    PERSON_ATTRIBUTES_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.person_outcomes import (
    PERSON_OUTCOMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.experiments.views.state_assignments import (
    STATE_ASSIGNMENTS_VIEW_BUILDER,
)

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = [
    CASE_TRIAGE_EVENTS_VIEW_BUILDER,
    CASE_TRIAGE_FEEDBACK_ACTIONS_VIEW_BUILDER,
    EXPERIMENTS_VIEW_BUILDER,
    OFFICER_ASSIGNMENTS_VIEW_BUILDER,
    OFFICER_ATTRIBUTES_VIEW_BUILDER,
    PERSON_ASSIGNMENTS_VIEW_BUILDER,
    PERSON_ATTRIBUTES_VIEW_BUILDER,
    PERSON_OUTCOMES_VIEW_BUILDER,
    STATE_ASSIGNMENTS_VIEW_BUILDER,
]
