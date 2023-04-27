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
"""Persons to a staff member that the person is affiliated with."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_PERSON_TO_STATE_STAFF_VIEW_NAME = "state_person_to_state_staff"

STATE_PERSON_TO_STATE_STAFF_DESCRIPTION = (
    """View with one row per state staff member a state person is affiliated with."""
)

STATE_PERSON_TO_STATE_STAFF_QUERY_TEMPLATE = """
SELECT
  DISTINCT person_id, staff_id, staff_external_id, staff_external_id_type, state_code
FROM (
  SELECT
    sp.person_id,
    sid.staff_id,
    sp.supervising_officer_staff_external_id AS staff_external_id,
    sp.supervising_officer_staff_external_id_type AS staff_external_id_type,
    sp.state_code
  FROM 
    `{project_id}.{state_base_dataset}.state_supervision_period` sp
  JOIN
    `{project_id}.{state_base_dataset}.state_staff_external_id` sid
  ON 
    sp.state_code = sid.state_code AND
    sp.supervising_officer_staff_external_id = sid.external_id AND 
    sp.supervising_officer_staff_external_id_type = sid.id_type

  UNION ALL 

  SELECT
    sa.person_id,
    sid.staff_id,
    sa.conducting_staff_external_id AS staff_external_id,
    sa.conducting_staff_external_id_type AS staff_external_id_type,
    sa.state_code
  FROM 
    `{project_id}.{state_base_dataset}.state_assessment` sa
  JOIN
    `{project_id}.{state_base_dataset}.state_staff_external_id` sid
  ON 
    sa.state_code = sid.state_code AND
    sa.conducting_staff_external_id = sid.external_id AND 
    sa.conducting_staff_external_id_type = sid.id_type

  UNION ALL 

  SELECT
    sc.person_id,
    sid.staff_id,
    sc.contacting_staff_external_id AS staff_external_id,
    sc.contacting_staff_external_id_type AS staff_external_id_type,
    sc.state_code
  FROM 
    `{project_id}.{state_base_dataset}.state_supervision_contact` sc
  JOIN
    `{project_id}.{state_base_dataset}.state_staff_external_id` sid
  ON 
    sc.state_code = sid.state_code AND
    sc.contacting_staff_external_id = sid.external_id AND 
    sc.contacting_staff_external_id_type = sid.id_type


  UNION ALL 

  SELECT
    pa.person_id,
    sid.staff_id,
    pa.referring_staff_external_id AS staff_external_id,
    pa.referring_staff_external_id_type AS staff_external_id_type,
    pa.state_code
  FROM 
    `{project_id}.{state_base_dataset}.state_program_assignment` pa
  JOIN
    `{project_id}.{state_base_dataset}.state_staff_external_id` sid
  ON 
    pa.state_code = sid.state_code AND
    pa.referring_staff_external_id = sid.external_id AND 
    pa.referring_staff_external_id_type = sid.id_type
)
"""
STATE_PERSON_TO_STATE_STAFF_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=STATE_PERSON_TO_STATE_STAFF_VIEW_NAME,
    view_query_template=STATE_PERSON_TO_STATE_STAFF_QUERY_TEMPLATE,
    description=STATE_PERSON_TO_STATE_STAFF_DESCRIPTION,
    state_base_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_PERSON_TO_STATE_STAFF_VIEW_BUILDER.build_and_print()
