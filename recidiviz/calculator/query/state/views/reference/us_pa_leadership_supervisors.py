# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""BQ View containing a roster of leadership folks in US_PA who also act as supervisors"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_LEADERSHIP_SUPERVISORS_NAME = "us_pa_leadership_supervisors"

US_PA_LEADERSHIP_SUPERVISORS_DESCRIPTION = """Provides a list of leadership folks in US_PA who also act as supervisors to be included in the Outliers supervision_officer_supervisors view
    """

US_PA_LEADERSHIP_SUPERVISORS_QUERY_TEMPLATE = """

  SELECT 
    DISTINCT
    super.state_code,
    super.supervisor_staff_external_id AS external_id,
    staff.staff_id,
    staff.full_name,
    staff.email,
    CASE 
        WHEN super.supervisor_staff_external_id = '459368' THEN '02'
        WHEN super.supervisor_staff_external_id = '534216' THEN '01'
    END AS supervision_district,
  FROM `{project_id}.{normalized_state_dataset}.state_staff_supervisor_period` super
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id` super_ids 
    ON super.supervisor_staff_external_id = super_ids.external_id 
    AND super.supervisor_staff_external_id_type = super_ids.id_type
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff` staff
    ON super_ids.staff_id = staff.staff_id
  WHERE 
    super.state_code = 'US_PA'
    AND super.supervisor_staff_external_id in ('534216', '459368')
    AND super.supervisor_staff_external_id_type = 'US_PA_PBPP_EMPLOYEE_NUM'
    AND super.end_date is null
"""

US_PA_LEADERSHIP_SUPERVISORS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_PA_LEADERSHIP_SUPERVISORS_NAME,
    view_query_template=US_PA_LEADERSHIP_SUPERVISORS_QUERY_TEMPLATE,
    description=US_PA_LEADERSHIP_SUPERVISORS_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_LEADERSHIP_SUPERVISORS_VIEW_BUILDER.build_and_print()
