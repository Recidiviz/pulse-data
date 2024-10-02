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
"""View with supervision contacts, keeping one contact per person-day-event_attributes.
"""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Supervision contacts, keeping one contact per person-day-event_attributes"
)

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.SUPERVISION_CONTACT,
    description=_VIEW_DESCRIPTION,
    sql_source=BigQueryAddress(
        dataset_id=NORMALIZED_STATE_DATASET, table_id="state_supervision_contact"
    ),
    attribute_cols=[
        "contact_type",
        "location",
        "status",
    ],
    event_date_col="contact_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
