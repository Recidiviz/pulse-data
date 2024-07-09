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
  - LBAKRDTA_TAK022 relates sentences to charges
  - LBAKRDTA_TAK025 to cross-reference sentences with sentence status snapshots
  - LBAKRDTA_TAK026 has the information for sentence status snapshots
"""
from recidiviz.ingest.direct.regions.us_mo.ingest_views.templates_sentences import (
    BS_BT_BU_IMPOSITION_FILTER,
    VALID_STATUS_CODES,
    VALID_SUPERVISION_SENTENCE_INITIAL_INFO,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
    -- Valid statuses are the base of hydrating this entity.
    valid_statuses AS ({VALID_STATUS_CODES}),
    -- These sentences have a charge and imposed_date
    valid_imposed AS (
        SELECT
            BS_DOC, -- unique for each person
            BS_CYC, -- unique for each sentence group
            BS_SEO  -- unique for each sentence
        FROM
            {{LBAKRDTA_TAK022}}
        LEFT JOIN
            ({VALID_SUPERVISION_SENTENCE_INITIAL_INFO})
        ON
            BS_DOC = BU_DOC AND
            BS_CYC = BU_CYC AND
            BS_SEO = BU_SEO
        LEFT JOIN
            {{LBAKRDTA_TAK023}}
        ON
            BS_DOC = BT_DOC AND 
            BS_CYC = BT_CYC AND
            BS_SEO = BT_SEO
        WHERE
            {BS_BT_BU_IMPOSITION_FILTER}
    )
SELECT 
    BS_DOC, -- Unique for each person
    BS_CYC, -- Unique for each sentence group
    BS_SEO, -- Unique for each sentence
    BW_SSO, -- status sequence num
    BW_SY,  -- status code change date
    BW_SCD, -- status code
    FH_SDE  -- status description
FROM
    valid_statuses
JOIN
    valid_imposed
USING
    (BS_DOC, BS_CYC, BS_SEO)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_status_snapshot",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
