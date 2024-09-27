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
"""View with transition to absconsion or bench warrant status events"""
from recidiviz.calculator.query.state.views.sessions.absconsion_bench_warrant_sessions import (
    ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType

_VIEW_DESCRIPTION = "Transition to absconsion or bench warrant status events"

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.ABSCONSION_BENCH_WARRANT,
    description=_VIEW_DESCRIPTION,
    sql_source=ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER.table_for_query,
    attribute_cols=[
        "inflow_from_level_1",
        "inflow_from_level_2",
    ],
    event_date_col="start_date",
)
