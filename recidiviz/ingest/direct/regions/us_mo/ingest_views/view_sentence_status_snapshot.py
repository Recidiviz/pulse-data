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
    STATUS_CODE_FILTERS,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Gets statuses that are not pre-trial and not in the future
FILTERED_STATUSES = f"""
SELECT
    BW_DOC AS DOC, -- person
    BW_CYC AS CYC, -- sentence group
    BW_SSO AS SSO, -- crosswalk number to link sentence to status
    BW_SY,  -- status code change date
    BW_SCD  -- status code
FROM
    {{LBAKRDTA_TAK026}}
WHERE 
    {STATUS_CODE_FILTERS}
"""

SENTENCE_SEQUENCE = """
SELECT
    BV_DOC AS DOC,
    BV_CYC AS CYC,
    BV_SSO AS SSO,
    CASE 
        WHEN BV_FSO = '0' THEN
            CONCAT(BV_DOC, '-', BV_CYC, '-', BV_SEO, '-', 'INCARCERATION')
        ELSE
            CONCAT(BV_DOC, '-', BV_CYC, '-', BV_SEO, '-', 'SUPERVISION')
    END AS sentence_external_id
FROM
    {LBAKRDTA_TAK025}
"""

VIEW_QUERY_TEMPLATE = f"""
WITH 
    filtered_sentence_statuses AS ({FILTERED_STATUSES}),
    sentence_sequence AS ({SENTENCE_SEQUENCE})
SELECT 
    DOC AS person_external_id,
    sentence_sequence.sentence_external_id,
    CAST(sentence_sequence.SSO AS INT64) AS BW_SSO,  -- status sequence num
    filtered_sentence_statuses.BW_SY,  -- status code change date
    filtered_sentence_statuses.BW_SCD  -- status code
FROM 
    sentence_sequence
JOIN 
    filtered_sentence_statuses
USING
    (DOC, CYC, SSO)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
