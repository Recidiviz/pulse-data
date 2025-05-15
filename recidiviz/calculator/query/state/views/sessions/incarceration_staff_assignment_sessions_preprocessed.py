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
"""Materialized view for incarceration staff assignments"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "incarceration_staff_assignment_sessions_preprocessed"
)

INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Materialized view for incarceration staff assignments"""
)
INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = """
    WITH legacy_relationships AS (      
      # TODO(#32754), TODO(#40128): Remove this pre-processing view when we've ingested 
      #  ME case manager relationships into state_person_staff_relationship_period 
      SELECT * FROM `{project_id}.sessions.us_me_incarceration_staff_assignment_sessions_preprocessed`
      
      UNION ALL
      
      # TODO(#32753): Remove this pre-processing view when we've ingested ND case manager 
      #  relationships into state_person_staff_relationship_period 
      SELECT * FROM `{project_id}.sessions.us_nd_incarceration_staff_assignment_sessions_preprocessed`
    )
    SELECT
      state_code,
      person_id,
      DATE(start_date) AS start_date,
      DATE(end_date_exclusive) AS end_date_exclusive,
      incarceration_staff_assignment_id,
      incarceration_staff_assignment_external_id,
      case_priority AS relationship_priority
    FROM
      legacy_relationships

    UNION ALL
    
    # TODO(#32754), TODO(#40128): Update to join agent_multiple_ids_map to support ME?
    SELECT 
        state_code,
        person_id,
        relationship_start_date AS start_date,
        relationship_end_date_exclusive AS end_date_exclusive,
        associated_staff_id AS incarceration_staff_assignment_id,
        associated_staff_external_id AS incarceration_staff_assignment_external_id,
        relationship_priority
    FROM `{project_id}.normalized_state.state_person_staff_relationship_period`
    WHERE state_code NOT IN (
        # TODO(#32754), TODO(#40128): Remove this filter when ME relationships are 
        #   ingested and we remove us_me_incarceration_staff_assignment_sessions_preprocessed above
        'US_ME', 
        # TODO(#32753): Remove this filter when ND relationships are ingested and we 
        #   remove us_nd_incarceration_staff_assignment_sessions_preprocessed above
        'US_ND'
    ) 
    AND system_type = 'INCARCERATION' AND relationship_type = 'CASE_MANAGER'
"""

INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
