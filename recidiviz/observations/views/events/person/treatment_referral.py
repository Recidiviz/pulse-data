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
"""View with treatment referrals, keeping at most one per person per
program-staff-status per day
"""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Treatment referrals, keeping at most one per person per program-staff-status per day"

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.TREATMENT_REFERRAL,
    description=_VIEW_DESCRIPTION,
    sql_source=BigQueryAddress(
        dataset_id=NORMALIZED_STATE_DATASET, table_id="state_program_assignment"
    ),
    attribute_cols=[
        "program_id",
        "referring_staff_id",
        "referral_metadata",
        "participation_status",
    ],
    event_date_col="referral_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()