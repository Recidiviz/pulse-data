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
"""Empolyment information for NE adults on parole."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Getting values from the decode table
codes AS (
  SELECT 
    codeId, 
    codeValue, 
  FROM {CodeValue}
), 
-- all relevant employment information
employment_info AS (
  SELECT 
    employmentId, 
    inmateNumber,
    employerName,
    jobTitle,
    addressLine1, 
    city,
    state,
    zipCode5,
    DATE(startDate) AS startDate,
    DATE(endDate) AS endDate,
    employmentStatus,
    employmentTypeCode,
    employmentStatusOther
  FROM {PIMSEmployment}
)
SELECT 
  employmentId, 
  inmateNumber,
  employerName,
  jobTitle,
  addressLine1, 
  city,
  s.codevalue AS state, 
  zipCode5,
  startDate,
  endDate,
  UPPER(c.codeValue) AS employmentStatus, 
  UPPER(t.codevalue) AS employmentTypeCode,
FROM employment_info e
LEFT JOIN codes c
ON e.employmentStatus = c.codeID
LEFT JOIN codes s
ON e.state = s.codeID
LEFT JOIN codes t
ON e.employmentTypeCode = t.codeID
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="employment_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
