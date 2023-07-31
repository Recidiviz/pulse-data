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
"""BQ View containing a supervisors in US_IX who also act as officers (have their own caseload)"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_DUAL_SUPERVISORS_OFFICERS_NAME = "us_ix_dual_supervisors_officers"

US_IX_DUAL_SUPERVISORS_OFFICERS_DESCRIPTION = """Provides a list of supervisors in US_IX who have their own caseloads and should appear in their supervisor's email
    """

US_IX_DUAL_SUPERVISORS_OFFICERS_QUERY_TEMPLATE = """
  SELECT
    DISTINCT
    attrs.state_code,
    officer_id as external_id,
    staff_id,
    staff.full_name,
    staff.email,
    supervisor_staff_external_id as supervisor_external_id,
    supervision_district,
    supervision_unit,
    specialized_caseload_type
  FROM `{project_id}.sessions.supervision_officer_attribute_sessions_materialized` attrs
  INNER JOIN `{project_id}.{normalized_state_dataset}.state_staff` staff USING(staff_id)
  WHERE 
    attrs.state_code = 'US_IX' AND
    officer_id IN (
      '3183',
      '3159',
      '2786',
      '3174',
      '2771',
      '2617',
      '2654',
      '2764',
      '2791',
      '2789'
    )
    AND (CURRENT_DATE("US/Pacific") BETWEEN start_date AND IFNULL(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), "9999-12-31") or officer_id = '3183')
    AND supervision_district IS NOT NULL
"""

US_IX_DUAL_SUPERVISORS_OFFICERS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_IX_DUAL_SUPERVISORS_OFFICERS_NAME,
    view_query_template=US_IX_DUAL_SUPERVISORS_OFFICERS_QUERY_TEMPLATE,
    description=US_IX_DUAL_SUPERVISORS_OFFICERS_DESCRIPTION,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_DUAL_SUPERVISORS_OFFICERS_VIEW_BUILDER.build_and_print()
