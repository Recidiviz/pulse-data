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
"""Query containing program assignment information from the following table:
RCDVZ_CISPRDDTA_CMOFFT
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- Getting program information from table CMOFFT
programs AS (
    SELECT DISTINCT
      RECORD_KEY, 
      TREAT_ID, 
      SUBFILE_KEY,
      EXIT_CODE, 
      REFER_DATE,
      ENTRY_DATE, 
      EXIT_DATE,
      LAST_UPDATE_LOCATION
    FROM {RCDVZ_CISPRDDTA_CMOFFT}
)
SELECT
  RECORD_KEY, 
  TREAT_ID, 
  SUBFILE_KEY,
  IF(TREAT_ID NOT LIKE '%%-%%', LAST_UPDATE_LOCATION, SPLIT(TREAT_ID, '-')[OFFSET(0)]) AS LOCATION,
  EXIT_CODE, 
  REFER_DATE,
  ENTRY_DATE, 
  EXIT_DATE
FROM programs
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_or",
    ingest_view_name="Program_Assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
