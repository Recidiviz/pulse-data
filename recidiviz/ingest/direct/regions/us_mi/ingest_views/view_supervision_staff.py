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
"""Query containing supervision staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
  omni.employee_id AS employee_id_omni,
  compas.RecId AS employee_id_compas,
  COALESCE(omni.first_name, compas.FirstName) AS first_name,
  COALESCE(omni.middle_name, compas.MiddleInitial) AS middle_name,
  COALESCE(omni.last_name, compas.LastName) AS last_name,
  COALESCE(omni.email_address, compas.Email) AS email_address,
FROM (
  SELECT e.employee_id, first_name, middle_name, last_name, email_address FROM {ADH_EMPLOYEE} e
  LEFT JOIN {ADH_EMPLOYEE_ADDITIONAL_INFO} ea
  ON e.employee_id = ea.employee_id
) omni
FULL JOIN (
  SELECT RecId, FirstName, MiddleInitial,LastName,Email FROM {ADH_SHUSER}
) compas
ON LOWER(omni.email_address) = LOWER(compas.Email)
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="supervision_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="employee_id_omni",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
