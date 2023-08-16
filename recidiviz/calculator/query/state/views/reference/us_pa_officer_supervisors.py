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
"""
BQ View containing a roster of POs in US_PA who also act as supervisors. These people
have appeared more recently in the case assignment history as officers, but are actively
supervising other officers so need to be included in the supervision_officer_supervisors
Outliers view.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_OFFICER_SUPERVISORS_NAME = "us_pa_officer_supervisors"

US_PA_OFFICER_SUPERVISORS_DESCRIPTION = """Provides a list of officers in US_PA who also act as supervisors to be included in the Outliers supervision_officer_supervisors view
    """

US_PA_OFFICER_SUPERVISORS_QUERY_TEMPLATE = """
  SELECT 
    DISTINCT
    officers.state_code,
    officers.supervisor_external_id AS external_id,
    staff.staff_id,
    staff.full_name,
    staff.email,
    CAST(NULL AS STRING) AS supervisor_external_id,
    CASE 
    -- One of these supervisors has an associated district, the other two required a workaround 
        WHEN TO_HEX(SHA256(officers.supervisor_external_id)) = 'ea63b7323e3b7f5a77b6fdb86bf45c35203035028187134e63e93fd5ee28f7d3'
        THEN LPAD(officers.supervision_district, 2, '0')
        ELSE '01'
    END AS supervision_district
  FROM `{project_id}.{outliers_views_dataset}.supervision_officers_materialized` officers
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id` super_ids 
    ON officers.supervisor_external_id = super_ids.external_id 
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff` staff
    ON super_ids.staff_id = staff.staff_id
  WHERE 
    officers.state_code = 'US_PA'
    AND TO_HEX(SHA256(officers.supervisor_external_id)) IN  (
        '8b52b5d1e87cf03b5693e2b8b4bf6874dea04b47817ac528364ab413cddc9ed4',  
        '29770299f86f56e36339b265308ff80faf2196d52d33d857baf38087ab232d07', 
        'ea63b7323e3b7f5a77b6fdb86bf45c35203035028187134e63e93fd5ee28f7d3'
        )
"""

US_PA_OFFICER_SUPERVISORS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_PA_OFFICER_SUPERVISORS_NAME,
    view_query_template=US_PA_OFFICER_SUPERVISORS_QUERY_TEMPLATE,
    description=US_PA_OFFICER_SUPERVISORS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    outliers_views_dataset=dataset_config.OUTLIERS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_OFFICER_SUPERVISORS_VIEW_BUILDER.build_and_print()
