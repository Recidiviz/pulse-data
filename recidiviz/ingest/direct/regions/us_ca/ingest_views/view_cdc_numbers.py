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
"""View that hydrates US_CA_CDCNO external ids, connecting them to any relevant
US_CA_DOC (OffenderId).
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Collects al valid CDC number and OffenderId pairs
all_cdc_numbers AS (
    SELECT OffenderId, CDCNO, DATE(IDVerifyDate) AS CDCNO_IDVERIFYDATE
    FROM {CDCNOInCustody}
    
    UNION ALL
    
    SELECT OffenderId, CDCNO, DATE(IDVERIFYDATE) AS CDCNO_IDVERIFYDATE
    FROM {CDCNOParole}
),
-- Choose the earliest verify date associated with each CDC number. As of 5/20/2025,
-- only one CDCNO has multiple conflicting IDVERIFYDATE values.
id_verify_dates AS (
    SELECT CDCNO, CDCNO_IDVERIFYDATE
    FROM all_cdc_numbers
    WHERE CDCNO_IDVERIFYDATE IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CDCNO ORDER BY CDCNO_IDVERIFYDATE
    ) = 1
)
SELECT
    OffenderId,
    CDCNO,
    CDCNO_IDVERIFYDATE
FROM (
    SELECT DISTINCT OffenderId, CDCNO 
    FROM all_cdc_numbers
)
JOIN id_verify_dates
USING (CDCNO)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="cdc_numbers",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
