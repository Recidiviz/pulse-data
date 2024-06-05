# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query produces a view for sentence status snapshots—points in time where a sentence status changes.
This includes:
  - Time of update
  - Type of status (COMPLETED, SERVING, REVOKED, COMMUTED, etc.)

Raw data files include:
  - LBAKRDTA_TAK025 to cross-reference sentences with sentence status snapshots
  - LBAKRDTA_TAK026 has the information for sentence status snapshots
"""
from recidiviz.ingest.direct.regions.us_mo.ingest_views.templates_sentences import (
    FROM_BU_BS_BV_BW_WHERE_NOT_PRETRIAL,
    MAGIC_DATES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_STATUSES = f"""
SELECT 
    supervision_statuses.*,
    FH_SDE -- status code description
FROM (
    SELECT 
        BV_DOC AS person_external_id,
        CONCAT(BV_DOC, '-', BV_CYC, '-', BV_SEO, '-', 'SUPERVISION') AS sentence_external_id,
        BW_SSO,  -- status sequence num
        BW_SY,  -- status code change date
        BW_SCD  -- status code
    {FROM_BU_BS_BV_BW_WHERE_NOT_PRETRIAL}
) supervision_statuses
JOIN
    {{LBAKRCOD_TAK146}} AS FH
ON
    supervision_statuses.BW_SCD = FH.FH_SCD
"""

# TODO(#28880) Remove the join to TAK022 BS here after raw data migration.
INCARCERATION_STATUSES = f"""
SELECT 
    BV_DOC AS person_external_id,
    CONCAT(BV_DOC, '-', BV_CYC, '-', BV_SEO, '-', 'INCARCERATION') AS sentence_external_id,
    BW_SSO,  -- status sequence num
    BW_SY,  -- status code change date
    BW_SCD,  -- status code
    FH_SDE,  -- status code description
FROM
    {{LBAKRDTA_TAK023}} AS BT
JOIN
    {{LBAKRDTA_TAK022}} AS BS
ON
    BT.BT_DOC = BS.BS_DOC AND
    BT.BT_CYC = BS.BS_CYC AND
    BT.BT_SEO = BS.BS_SEO
JOIN
    {{LBAKRDTA_TAK025}} AS BV
ON
    BT.BT_DOC = BV.BV_DOC AND
    BT.BT_CYC = BV.BV_CYC AND
    BT.BT_SEO = BV.BV_SEO
JOIN
     {{LBAKRDTA_TAK026}} AS BW
ON
    BV.BV_DOC = BW.BW_DOC AND
    BV.BV_CYC = BW.BW_CYC AND
    BV.BV_SSO = BW.BW_SSO
JOIN
    {{LBAKRCOD_TAK146}} AS FH
ON
    BW.BW_SCD = FH.FH_SCD
WHERE
    -- Incarceration statuses have an FSO of 0
    BV_FSO = '0'
AND
    -- We get weekly data with expected statuses for the (future) week,
    -- so this ensures we only get statuses that have happened
    CAST(BW_SY AS INT64) <= CAST(FORMAT_DATE('%Y%m%d', CURRENT_TIMESTAMP()) AS INT64)
AND
    -- sentence must have valid imposed_date
    BT_SD NOT IN {MAGIC_DATES}
"""

VIEW_QUERY_TEMPLATE = f"""
WITH 
    inc_status AS ({INCARCERATION_STATUSES}),
    sup_status AS ({SUPERVISION_STATUSES})
SELECT * FROM inc_status
UNION ALL
SELECT * FROM sup_status
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
