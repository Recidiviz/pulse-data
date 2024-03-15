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
  - LBAKRDTA_TAK022 has the base information for incarceration sentences
  - LBAKRDTA_TAK025 to cross-reference sentences with sentence status snapshots
  - LBAKRDTA_TAK026 has the information for sentence status snapshots
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#26620) Include supervision status info, BU_FSO <-> BV_FSO. Incarceration has an implicit _FSO = 0
VIEW_QUERY_TEMPLATE = """
SELECT 
    base_sentence.BS_DOC,                         -- unique for each person
    base_sentence.BS_CYC,                         -- unique for each sentence group
    base_sentence.BS_SEO,                         -- unique for each sentence
    CAST(sent_status.BW_SSO AS INT64) AS BW_SSO,  -- status sequence num
    base_sentence.BS_SCF,                         -- sentence is completed
    sent_status.BW_SCD,                           -- status code
    sent_status.BW_SY                             -- status code change date
FROM 
    {LBAKRDTA_TAK022} AS base_sentence
JOIN 
    {LBAKRDTA_TAK025} AS sent_crossref
ON 
    base_sentence.BS_DOC = sent_crossref.BV_DOC AND 
    base_sentence.BS_CYC = sent_crossref.BV_CYC AND 
    base_sentence.BS_SEO = sent_crossref.BV_SEO 
JOIN 
    {LBAKRDTA_TAK026} AS sent_status
ON 
    sent_crossref.BV_DOC = sent_status.BW_DOC AND 
    sent_crossref.BV_CYC = sent_status.BW_CYC AND 
    sent_crossref.BV_SSO = sent_status.BW_SSO
WHERE 
    CAST(sent_status.BW_SY AS INT64) <= CAST(FORMAT_DATE('%Y%m%d', CURRENT_TIMESTAMP()) AS INT64)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BS_DOC, BS_CYC, BS_SEO, BW_SSO",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
