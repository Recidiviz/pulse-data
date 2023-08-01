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
    attrs.officer_id as external_id,
    TO_HEX(SHA256(attrs.officer_id)) as officer_id_hash,
    staff_id,
    staff.full_name,
    staff.email,
    CASE WHEN TO_HEX(SHA256(attrs.officer_id)) = '92a7194ae5db4ecb83f724e83d0d50b3c216561849b48f4d60946b3b0a301a3a'
         THEN missing_supervisor.officer_id
      ELSE supervisor_staff_external_id
      END as supervisor_external_id,
    supervision_district,
    supervision_unit,
    specialized_caseload_type
  FROM `{project_id}.sessions.supervision_officer_attribute_sessions_materialized` attrs
  INNER JOIN `{project_id}.{normalized_state_dataset}.state_staff` staff USING(staff_id)
  LEFT JOIN (
    select officer_id 
    from `{project_id}.sessions.supervision_officer_attribute_sessions_materialized` 
    where TO_HEX(SHA256(officer_id)) = 'a33b4b5dd71ecdc0d1e24f636fdaaff8902a3a0f8b0604bcfadb6ae9adea15de'
  ) missing_supervisor ON 1=1
  WHERE 
    attrs.state_code = 'US_IX' AND
    TO_HEX(SHA256(attrs.officer_id)) IN (
    '0a6b81782b5c6d04236250f24ed2b3a27c3afd1a80371ea238ba029cbe5aca0d',
    '13d61412db18277db74db4337a034d6972da028bdcf41c2286c07f452753fc8f',
    '144dc53d5dff011b70e273ff282f86c2976c8268811315f548ae013705941ba8',
    '6bcc05d40b1752992b7142914608fbac9adb28af8ca29b7d14b1e46428be693e',
    '7929065522441d4053cba7ebffb2d224585a110b13840ff69cf0e89b725af9e7',
    '92a7194ae5db4ecb83f724e83d0d50b3c216561849b48f4d60946b3b0a301a3a',
    'dbaf899fbd964344fefdd5d21bb73a9b0b1654799a0a430106e87ec5b6300a77',
    'df979f8f4738e382b5e7c616a20b341ca41cece8dce7532c2c50490109bf43f0',
    'e172e24d3024bb7c229c66312fe18a56c169c8da74e48c666e1590141cec58e5',
    'ef5839d610a3567ed8e2b6df29dce544e3fb9ffbaf45f507b3a69fbc5e81f507'
    )
    AND (CURRENT_DATE("US/Pacific") BETWEEN start_date AND IFNULL(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), "9999-12-31") or TO_HEX(SHA256(attrs.officer_id))= '92a7194ae5db4ecb83f724e83d0d50b3c216561849b48f4d60946b3b0a301a3a')
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
