# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Identity ingest view for the US_ND Elite booking<->person association.

One row per person keyed on the cleaned Elite ROOT_OFFENDER_ID, with every
booking ID the person has held aggregated into a comma-separated
BOOKING_ID_LIST. The resulting fragment carries only external IDs (no
attributes): the person's US_ND_ELITE ID plus one US_ND_ELITE_BOOKING ID per
booking. A person routinely holds several bookings (one per new series of
justice-system interactions), so the view aggregates them onto the person's
single row rather than emitting one row per booking, per the
one-row-per-person requirement on identity views (see
FilterSentinelExternalIds).
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    ROOT_OFFENDER_ID,
    STRING_AGG(OFFENDER_BOOK_ID, ',' ORDER BY OFFENDER_BOOK_ID) AS BOOKING_ID_LIST
FROM (
    SELECT
        REGEXP_REPLACE(ROOT_OFFENDER_ID, r'\\.00$|,', '') AS ROOT_OFFENDER_ID,
        OFFENDER_BOOK_ID
    FROM {elite_offenderbookingstable}
    WHERE ROOT_OFFENDER_ID IS NOT NULL AND OFFENDER_BOOK_ID IS NOT NULL
)
GROUP BY ROOT_OFFENDER_ID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_offenderbookingstable",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
