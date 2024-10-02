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
"""View with spans between completed contact dates and the subsequent contact date to
 identify the most recent completed contact"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Spans between completed contact dates and the subsequent contact date to help "
    "identify the most recent completed contact"
)
_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    contact_date,
    LEAD(contact_date) OVER (
        PARTITION BY person_id ORDER BY contact_date
    ) AS next_completed_contact_date
FROM (
    SELECT DISTINCT
        state_code,
        person_id,
        contact_date
    FROM `{project_id}.normalized_state.state_supervision_contact`
    WHERE status = "COMPLETED"
)
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.COMPLETED_CONTACT_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[],
    span_start_date_col="contact_date",
    span_end_date_col="next_completed_contact_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
