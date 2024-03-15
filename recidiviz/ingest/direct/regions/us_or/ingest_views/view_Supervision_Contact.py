# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query containing supervision period information from the following tables:

"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH people_on_supervision AS (
# finds all people with statuses for supervision that have not been terminated or are not currently unsupervised status (UNSU)
# Limits all contact to people currently on superivison, is currently the desired functionality. Change if we need historical in future
  SELECT DISTINCT 
    RECORD_KEY, 
    CURRENT_STATUS, 
    RESPONSIBLE_LOCATION, 
    ID_NUMBER, 
    COMMUNITY_SUPER_LVL, 
    CASELOAD, 
    (DATE(ADMISSION_DATE)) AS ADMISSION_DATE
  FROM {RCDVZ_PRDDTA_OP013P}
  WHERE CURRENT_STATUS IN ('PS', 'PR', 'PO', 'CD', 'DV', 'PA', 'SL')
  AND OUTCOUNT_REASON IS NULL OR OUTCOUNT_REASON = 'UNSU'
),
contacts AS (
  SELECT DISTINCT
    RECORD_KEY,
    cm.CASELOAD, 
    NEXT_NUMBER,
    CHRONO_WHO, 
    CHRONO_TYPE, 
    CHRONO_WHAT, 
    (DATE(CHRONO_DATE)) AS CHRONO_DATE
  FROM {RCDVZ_CISPRDDTA_CMCROH} cm
  WHERE CHRONO_WHO = 'O' # Contact for Adult in Custody or Adult in Supervision
  AND CHRONO_TYPE IN ('O', 'OV', 'H', 'CORT', 'DAYR', 'E', 'FLD', 'J', 'TV', 'VV', 'TX') 
  AND cm.CASELOAD IS NOT null
  AND RECORD_KEY IN (SELECT DISTINCT RECORD_KEY FROM people_on_supervision)
)
SELECT DISTINCT
  RECORD_KEY, 
  ps.CASELOAD, 
  NEXT_NUMBER,
  CHRONO_WHO, 
  CHRONO_TYPE, 
  CHRONO_WHAT, 
  CHRONO_DATE
FROM people_on_supervision ps
LEFT JOIN contacts
USING (RECORD_KEY)
WHERE CHRONO_DATE >= ADMISSION_DATE
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Supervision_Contact",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="RECORD_KEY, CHRONO_DATE",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
