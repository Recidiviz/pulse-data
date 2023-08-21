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
"""BQ View containing a roster of leadership folks in US_IX who also act as supervisors"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_LEADERSHIP_SUPERVISORS_NAME = "us_ix_leadership_supervisors"

US_IX_LEADERSHIP_SUPERVISORS_DESCRIPTION = """Provides a list of leadership folks in US_IX who also act as supervisors to be included in the Outliers supervision_officer_supervisors view
    """

US_IX_LEADERSHIP_SUPERVISORS_QUERY_TEMPLATE = """

  SELECT 
    DISTINCT
    super.state_code,
    super.supervisor_staff_external_id AS external_id,
    TO_HEX(SHA256(super.supervisor_staff_external_id)) AS supervisor_id_hash,
    staff.staff_id,
    staff.full_name,
    staff.email,
    CAST(NULL AS STRING) AS supervisor_external_id,
    CASE WHEN TO_HEX(SHA256(supervisor_staff_external_id)) = '5f829f9e9b41706a703e4b4d3b89c31f34acaf80ffbe3ed4ac1431c4527800f2' THEN 'DISTRICT 2'
         ELSE JSON_EXTRACT_SCALAR(loc_meta.location_metadata, '$.supervision_district_name') 
      END AS supervision_district,
    CAST(NULL AS STRING) AS supervision_unit,  
  FROM `{project_id}.{normalized_state_dataset}.state_staff_supervisor_period` super
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id` super_ids 
    ON super.supervisor_staff_external_id = super_ids.external_id 
        AND super.supervisor_staff_external_id_type = super_ids.id_type
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff` staff
    ON super_ids.staff_id = staff.staff_id
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_location_period` loc
    ON super_ids.staff_id = loc.staff_id  
        AND loc.end_date is null
  LEFT JOIN `{project_id}.{reference_views_dataset}.location_metadata_materialized` loc_meta
    ON loc.location_external_id = loc_meta.location_external_id
  WHERE 
    super.state_code = 'US_IX'
    AND TO_HEX(SHA256(super.supervisor_staff_external_id)) in (
      'ce121073a5210988d0d44892ed9597a853b6f7384ef3887c0c4c229dbabb4691', 
      '6bcc05d40b1752992b7142914608fbac9adb28af8ca29b7d14b1e46428be693e', 
      '2acee0ce6421d1b64d86cc141ef89090305ec18652dcf7d264cb034739e2c1b1', 
      'e5c31b7a40373b6459fad9f33059f767665b7028f375ac63a8279ad53a2c5488', 
      '5f829f9e9b41706a703e4b4d3b89c31f34acaf80ffbe3ed4ac1431c4527800f2')
    AND super.supervisor_staff_external_id_type = 'US_IX_EMPLOYEE'
    AND super.end_date is null
"""

US_IX_LEADERSHIP_SUPERVISORS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_IX_LEADERSHIP_SUPERVISORS_NAME,
    view_query_template=US_IX_LEADERSHIP_SUPERVISORS_QUERY_TEMPLATE,
    description=US_IX_LEADERSHIP_SUPERVISORS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_LEADERSHIP_SUPERVISORS_VIEW_BUILDER.build_and_print()
