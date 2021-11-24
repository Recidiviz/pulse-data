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
"""Query containing external movements information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH in_edges AS (
    SELECT * FROM {elite_externalmovements}
    WHERE DIRECTION_CODE = 'IN'
), out_edges AS (
    SELECT * FROM {elite_externalmovements}
    WHERE DIRECTION_CODE = 'OUT'
), full_periods AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY COALESCE(in_edges.OFFENDER_BOOK_ID, out_edges.OFFENDER_BOOK_ID)
                       ORDER BY CAST(COALESCE(in_edges.MOVEMENT_SEQ, out_edges.MOVEMENT_SEQ) AS INT64)
                       ) as period_sequence,
        IFNULL(in_edges.OFFENDER_BOOK_ID, out_edges.OFFENDER_BOOK_ID) AS offender_book_id,
        in_edges.MOVEMENT_REASON_CODE AS admission_reason_code,
        in_edges.MOVEMENT_DATE AS admission_date,
        IFNULL(in_edges.TO_AGY_LOC_ID, out_edges.FROM_AGY_LOC_ID) as facility,
        out_edges.MOVEMENT_DATE AS release_date,
        out_edges.MOVEMENT_REASON_CODE AS release_reason_code
    FROM
        in_edges
    FULL OUTER JOIN 
        out_edges
    -- Only join movements that are related to the same booking ID
    ON in_edges.OFFENDER_BOOK_ID = out_edges.OFFENDER_BOOK_ID
    -- Only join consecutive movement sequences
    AND CAST(in_edges.MOVEMENT_SEQ AS INT) = (CAST(out_edges.MOVEMENT_SEQ AS INT) - 1)
    -- Only join movements if the facilities match
    AND in_edges.TO_AGY_LOC_ID = out_edges.FROM_AGY_LOC_ID
), missing_releases_handled AS (
    SELECT
        * EXCEPT(release_date),
        -- If there's no release but we know that the movement is not the person's most
        -- recent movement then use the next admission as the release
        CASE
            WHEN release_date IS NULL
                THEN LEAD(admission_date) OVER (PARTITION BY offender_book_id ORDER BY period_sequence)
            ELSE release_date 
        END AS release_date
    FROM full_periods
)

SELECT
    *
FROM missing_releases_handled
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_nd",
    ingest_view_name="elite_externalmovements_incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="offender_book_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
