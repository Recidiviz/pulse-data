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
"""Identity ingest view for the US_CO DOC badge<->party association.

One row per DOC badge number, with every PARTYID that currently holds the badge
aggregated into a comma-separated PARTYID_LIST. The resulting fragment carries
only external IDs (no attributes): the badge's US_CO_DOC_BADGE_NUMBER plus one
US_CO_PARTYID per party record. A staff member who re-enters the eomis system
gets a new PARTYID but keeps their badge, so the view aggregates the parties
onto the badge's single row rather than emitting one row per party, per the
one-row-per-person requirement on identity views (see
FilterSentinelExternalIds). Clustering merges each party's eomis_staff roster
fragment into one staff member through this fragment's shared ids.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    DOC_BADGE_NUMBER,
    STRING_AGG(PARTYID, ',' ORDER BY PARTYID) AS PARTYID_LIST
FROM (
    -- DOC badge numbers (PTYIDTYPE '35', agency CO). In the rare case a party
    -- carries multiple badge rows, keep the most recently updated one, matching
    -- the activity StateStaff view, so each PARTYID links only its current
    -- badge and lands on exactly one fragment of this view.
    SELECT
        PARTYID,
        OTHERID AS DOC_BADGE_NUMBER
    FROM {eomis_ptyotherid}
    WHERE PTYIDTYPE = '35'
        AND PTYIDAGENCY = 'CO'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PARTYID
        -- OTHERID breaks ties between badge rows sharing an update time, so the
        -- pick stays deterministic.
        ORDER BY
            DATETIME(DATE(DATELASTUPDATE), PARSE_TIME('%X', TIMELASTUPDATE)) DESC,
            OTHERID
    ) = 1
)
GROUP BY DOC_BADGE_NUMBER
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_co",
    ingest_view_name="eomis_staff_badges",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
