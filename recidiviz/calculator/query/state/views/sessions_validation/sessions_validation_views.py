# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Defines a collection of views that are used to help validate sessions dataset data,
e.g. in Looker dashboards.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_admissions_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_population_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_incarceration_releases_to_dataflow_disaggregated import (
    SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_out_of_state_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_population_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_starts_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions_validation.session_supervision_terminations_to_dataflow_disaggregated import (
    SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
)

SESSIONS_VALIDATION_VIEW_BUILDERS: list[SimpleBigQueryViewBuilder] = [
    SESSION_INCARCERATION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
    SESSION_SUPERVISION_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
    SESSION_SUPERVISION_OUT_OF_STATE_POPULATION_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
    SESSION_INCARCERATION_ADMISSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
    SESSION_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
    SESSION_INCARCERATION_RELEASES_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
    SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER,
]
