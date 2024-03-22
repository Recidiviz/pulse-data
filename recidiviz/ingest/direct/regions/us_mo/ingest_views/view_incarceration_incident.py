# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing conduct violation information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    cdv.IZ_DOC AS DOC_ID,
    cdv.IZ_CYC AS CYCLE_NO,
    cdv.IZCSEQ AS CDV_SEQ,
    cdv.IZ_II AS CDV_EVENT_DATE,
    cdv.IZWDTE AS CDV_DATE_WRITTEN,
    cdv.IZVRUL AS CDV_RULE,
    cdv.IZ_PLN AS CDV_DOC_LOCATION,
    cdv.IZ_LOC AS CDV_BUILDING,

    event.IVEDTE AS DISP_DATE,

    sanc.IUSASQ AS SANC_SEQ,
    sanc.IUSSAN AS SANC_CODE,
    NULLIF(sanc.IU_SU,'0') AS SANC_BEGIN_DATE,
    NULLIF(sanc.IU_SE,'0') AS SANC_EXP_DATE,
    sanc.IUSMTH AS SANC_MONTHS,
    sanc.IU_SAD AS SANC_DAYS,
    sanc.IU_SHR AS SANC_HOURS
FROM {LBAKRDTA_TAK233} cdv

LEFT JOIN (
    SELECT * EXCEPT(disp_recency) 
    FROM (
        SELECT DISTINCT
            IV_DOC,
            IV_CYC,
            IVCSEQ,
            IVEDTE,
            ROW_NUMBER() OVER (PARTITION BY IV_DOC,IV_CYC,IVCSEQ ORDER BY IVEDTE DESC) disp_recency
        FROM {LBAKRDTA_TAK237}
        WHERE IVETYP = 'T'
        -- Select dates corresponding to disposition events only
    ) event_dup_dates
    WHERE disp_recency = 1
    -- Select only the most recent disposition date
) event 
ON 
  cdv.IZ_DOC = event.IV_DOC
  AND cdv.IZ_CYC = event.IV_CYC
  AND cdv.IZCSEQ = event.IVCSEQ

LEFT JOIN {LBAKRDTA_TAK236} sanc
ON 
  cdv.IZ_DOC = sanc.IU_DOC
  AND cdv.IZ_CYC = sanc.IU_CYC
  AND cdv.IZCSEQ = sanc.IUCSEQ
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="incarceration_incident",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
