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
"""Query containing supervision violation information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH offenses AS (
  SELECT
    OFFENDERID,
    INTERVENTIONDATE,
    INTERVENTIONTIME,
    OFFENSESEQ,
    NULLIF(OFFENSEDATE, '1000-01-01 00:00:00') AS OFFENSEDATE,
    OFFENSECODE
  FROM {INTERVENTOFFENSE}
),
sanctions AS (
  SELECT
    DISTINCT
    sanc.OFFENDERID,
    sanc.INTERVENTIONDATE,
    sanc.INTERVENTIONTIME,
    sanc.SANCTIONSEQ,
    sanc.SANCTIONTYPE,
    ivn.PPOFFICERID
  FROM {INTERVENTSANCTION@ALL} sanc
  LEFT JOIN {SUPVINTERVENTION} ivn
  USING(OFFENDERID,INTERVENTIONDATE,INTERVENTIONTIME)
  WHERE ivn.INTERVENTIONSTATUS = 'A'
),
offenses_and_sanctions AS (
SELECT *
FROM offenses
LEFT JOIN sanctions
USING(OFFENDERID,INTERVENTIONDATE,INTERVENTIONTIME)
)

SELECT 
    OFFENDERID,
    INTERVENTIONDATE,
    INTERVENTIONTIME,
    OFFENSESEQ,
    OFFENSEDATE,
    OFFENSECODE,
    TO_JSON_STRING(ARRAY_AGG(STRUCT<SANCTIONSEQ string,SANCTIONTYPE string,PPOFFICERID string>(SANCTIONSEQ,SANCTIONTYPE,PPOFFICERID) ORDER BY SANCTIONSEQ)) AS sanction_list
FROM offenses_and_sanctions 
GROUP BY 
    OFFENDERID,
    INTERVENTIONDATE,
    INTERVENTIONTIME,
    OFFENSESEQ,
    OFFENSEDATE,
    OFFENSECODE
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="supervision_violation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
