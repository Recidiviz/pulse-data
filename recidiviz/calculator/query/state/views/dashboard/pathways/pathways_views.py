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
"""Dashboard views related to pathways."""
from typing import List, Set

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.liberty_to_prison_transitions import (
    LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_by_dimension import (
    PRISON_POPULATION_BY_DIMENSION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_over_time import (
    PRISON_POPULATION_OVER_TIME_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_person_level import (
    PRISON_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_projection import (
    PRISON_POPULATION_PROJECTION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_to_supervision_transitions import (
    PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population import (
    SUPERVISION_POPULATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population_by_dimension import (
    SUPERVISION_POPULATION_BY_DIMENSION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population_over_time import (
    SUPERVISION_POPULATION_OVER_TIME_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population_projection import (
    SUPERVISION_POPULATION_PROJECTION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_to_liberty_transitions import (
    SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_to_prison_transitions import (
    SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_to_prison_transitions_raw import (
    SUPERVISION_TO_PRISON_TRANSITIONS_RAW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_incarceration_location_name_map import (
    PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_supervision_location_name_map import (
    PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.supervision_to_prison_population_snapshot_by_officer import (
    SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_BUILDER,
)

# If adding a PRISON module specific view builder to this list, also add it to the PATHWAYS_PRISON export in products.yaml
PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS: List[
    WithMetadataQueryBigQueryViewBuilder[SelectedColumnsBigQueryViewBuilder]
] = [
    LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
    PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER,
    SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_BUILDER,
    SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
    PRISON_POPULATION_PROJECTION_VIEW_BUILDER,
    PRISON_POPULATION_OVER_TIME_VIEW_BUILDER,
    PRISON_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
    PRISON_POPULATION_BY_DIMENSION_VIEW_BUILDER,
    SUPERVISION_POPULATION_PROJECTION_VIEW_BUILDER,
    SUPERVISION_POPULATION_OVER_TIME_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_DIMENSION_VIEW_BUILDER,
]

PATHWAYS_EVENT_LEVEL_VIEW_BUILDER_HELPERS: Set[SelectedColumnsBigQueryViewBuilder] = {
    SUPERVISION_POPULATION_VIEW_BUILDER,
    SUPERVISION_TO_PRISON_TRANSITIONS_RAW_VIEW_BUILDER,
}

PATHWAYS_SUPERVISION_TO_PRISON_VIEW_BUILDERS: List[
    PathwaysMetricBigQueryViewBuilder
] = [
    SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_BUILDER,
]

PATHWAYS_VIEW_BUILDERS: List[PathwaysMetricBigQueryViewBuilder] = [
    *PATHWAYS_SUPERVISION_TO_PRISON_VIEW_BUILDERS,
]

PATHWAYS_HELPER_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    *PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
    *PATHWAYS_EVENT_LEVEL_VIEW_BUILDER_HELPERS,
    PATHWAYS_SUPERVISION_LOCATION_NAME_MAP_VIEW_BUILDER,
    PATHWAYS_INCARCERATION_LOCATION_NAME_MAP_VIEW_BUILDER,
]
