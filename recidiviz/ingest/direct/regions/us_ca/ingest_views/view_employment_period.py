# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Query containing CDCR employment period information. For more information,
including to-do's and things we could UXR, see us_ca.md.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  OffenderId,
  ROW_NUMBER() OVER (PARTITION BY OffenderId ORDER BY CAST(EMPSTARTDATE AS DATETIME), EmpAddress, EmpCityState ASC) AS sequence_number,
  EMPSTARTDATE,
  IF(CAST(EMPLENDDATE AS DATETIME) = CAST('9999-12-31 00:00:00' AS DATETIME), NULL, EMPLENDDATE) AS EMPLENDDATE,
  EMPLOYMENTTYPE,
  UPPER(TRIM(EmployerName)) AS EmployerName,
  CASE WHEN EmpAddress is not null and EmpCityState is not null 
    THEN CONCAT(EmpAddress, ', ', EmpCityState)
    ELSE COALESCE(EmpAddress, EmpCityState)
  END AS address
FROM {ParoleEmployment}
"""

#
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="employment_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
