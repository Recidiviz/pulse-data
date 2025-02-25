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
"""View with sssignment to a tracked experiment, with one variant assignment per
person-day-experiment-variant.
"""
from recidiviz.calculator.query.experiments_metadata.views.person_assignments import (
    PERSON_ASSIGNMENTS_VIEW_BUILDER,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Assignment to a tracked experiment, with one variant assignment per person-day-experiment-variant"


VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.VARIANT_ASSIGNMENT,
    description=_VIEW_DESCRIPTION,
    sql_source=PERSON_ASSIGNMENTS_VIEW_BUILDER.table_for_query,
    attribute_cols=["experiment_id", "variant_id"],
    event_date_col="variant_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
